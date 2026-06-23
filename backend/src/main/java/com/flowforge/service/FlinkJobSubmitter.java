package com.flowforge.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Submits Flink JAR jobs to the Flink REST API and polls until completion.
 *
 * Flow:
 *  1. Upload JAR (if not already uploaded in this JVM session)
 *  2. POST /jars/{jarId}/run with program args
 *  3. Poll GET /jobs/{jobId}/overview until FINISHED or FAILED
 *  4. Return number of records processed (from job metrics if available)
 */
@Service
@Slf4j
public class FlinkJobSubmitter {

    @Value("${flink.rest.url:http://flink-jobmanager:8081}")
    private String flinkRestUrl;

    @Value("${flink.jars.dir:/app/flink-jars}")
    private String flinkJarsDir;

    private static final String JAR_FILENAME = "flowforge-flink-jobs.jar";
    private static final Duration POLL_INTERVAL = Duration.ofSeconds(3);
    private static final Duration JOB_TIMEOUT   = Duration.ofSeconds(120);

    private final HttpClient http = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .build();
    private final ObjectMapper mapper = new ObjectMapper();

    // Cache the uploaded jar ID within the process lifetime to avoid re-uploading
    private volatile String cachedJarId = null;

    /**
     * Submits a Flink filter job and blocks until it finishes.
     *
     * @param inputTopic  Kafka topic with input records
     * @param outputTopic Kafka topic to write filtered records to
     * @param bootstrap   Kafka bootstrap servers (host:port)
     * @param field       JSON field name to filter on
     * @param operator    Comparison operator: ==, !=, >, <, contains
     * @param value       Value to compare against
     * @param executionId FlowForge execution ID (used as consumer group)
     * @return Number of records that matched the filter (approximated from job metrics)
     */
    public long submitFilterJob(String inputTopic, String outputTopic, String bootstrap,
                                 String field, String operator, String value,
                                 String executionId) throws Exception {
        String jarId = ensureJarUploaded();
        List<String> args = List.of(
                "--input-topic",  inputTopic,
                "--output-topic", outputTopic,
                "--bootstrap",    bootstrap,
                "--field",        field,
                "--operator",     operator,
                "--value",        value,
                "--execution-id", executionId
        );
        return runJobAndWait(jarId, "com.flowforge.flink.FilterJob", args, executionId);
    }

    /**
     * Submits a Flink aggregator job and blocks until it finishes.
     */
    public long submitAggregatorJob(String inputTopic, String outputTopic, String bootstrap,
                                     String groupBy, String function, String field,
                                     String executionId) throws Exception {
        String jarId = ensureJarUploaded();
        List<String> args = List.of(
                "--input-topic",  inputTopic,
                "--output-topic", outputTopic,
                "--bootstrap",    bootstrap,
                "--group-by",     groupBy,
                "--function",     function,
                "--field",        field,
                "--window-size",  "10",
                "--execution-id", executionId
        );
        return runJobAndWait(jarId, "com.flowforge.flink.AggregatorJob", args, executionId);
    }

    // ── Internal helpers ──────────────────────────────────────────────────────────

    private String ensureJarUploaded() throws Exception {
        if (cachedJarId != null) {
            // Verify it still exists on the Flink cluster
            if (jarExists(cachedJarId)) return cachedJarId;
            cachedJarId = null;
        }
        cachedJarId = uploadJar();
        return cachedJarId;
    }

    private boolean jarExists(String jarId) {
        try {
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create(flinkRestUrl + "/jars"))
                    .GET().build();
            HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
            return resp.body().contains(jarId);
        } catch (Exception e) {
            return false;
        }
    }

    private String uploadJar() throws Exception {
        Path jarPath = Path.of(flinkJarsDir, JAR_FILENAME);
        if (!Files.exists(jarPath)) {
            throw new IllegalStateException("Flink JAR not found at " + jarPath +
                    ". Ensure flink-jar-builder service has completed.");
        }

        byte[] jarBytes = Files.readAllBytes(jarPath);

        // Multipart upload — Flink REST expects multipart/form-data
        String boundary = "FlowForgeBoundary";
        byte[] header = ("--" + boundary + "\r\n" +
                "Content-Disposition: form-data; name=\"jarfile\"; filename=\"" + JAR_FILENAME + "\"\r\n" +
                "Content-Type: application/java-archive\r\n\r\n").getBytes();
        byte[] footer = ("\r\n--" + boundary + "--\r\n").getBytes();

        byte[] body = new byte[header.length + jarBytes.length + footer.length];
        System.arraycopy(header,   0, body, 0,                              header.length);
        System.arraycopy(jarBytes, 0, body, header.length,                  jarBytes.length);
        System.arraycopy(footer,   0, body, header.length + jarBytes.length, footer.length);

        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(flinkRestUrl + "/jars/upload"))
                .header("Content-Type", "multipart/form-data; boundary=" + boundary)
                .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                .build();

        HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() != 200) {
            throw new RuntimeException("Flink JAR upload failed: " + resp.statusCode() + " " + resp.body());
        }

        JsonNode json = mapper.readTree(resp.body());
        String filename = json.get("filename").asText(); // e.g. "/tmp/flink-jars/abc123_flowforge-flink-jobs.jar"
        String jarId = Path.of(filename).getFileName().toString();
        // Flink returns path like "/tmp/.../uuid_filename.jar"; the jarId is just the filename part
        log.info("Uploaded Flink JAR: {} → jarId={}", JAR_FILENAME, jarId);
        return jarId;
    }

    private long runJobAndWait(String jarId, String mainClass, List<String> args, String executionId) throws Exception {
        // Build program args string
        List<String> flatArgs = new ArrayList<>(args);
        String programArgs = String.join(" ", flatArgs);

        Map<String, Object> runBody = Map.of(
                "entryClass", mainClass,
                "programArgs", programArgs,
                "parallelism", 1
        );

        HttpRequest runReq = HttpRequest.newBuilder()
                .uri(URI.create(flinkRestUrl + "/jars/" + jarId + "/run"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(mapper.writeValueAsString(runBody)))
                .build();

        HttpResponse<String> runResp = http.send(runReq, HttpResponse.BodyHandlers.ofString());
        if (runResp.statusCode() != 200) {
            throw new RuntimeException("Flink job submission failed: " + runResp.statusCode() + " " + runResp.body());
        }

        String jobId = mapper.readTree(runResp.body()).get("jobid").asText();
        log.info("Flink job submitted: jobId={} executionId={}", jobId, executionId);

        return pollUntilDone(jobId);
    }

    private long pollUntilDone(String jobId) throws Exception {
        long deadline = System.currentTimeMillis() + JOB_TIMEOUT.toMillis();

        while (System.currentTimeMillis() < deadline) {
            Thread.sleep(POLL_INTERVAL.toMillis());

            // Flink 1.18 REST API: GET /jobs/{jobid}  (not /jobs/{jobid}/overview)
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create(flinkRestUrl + "/jobs/" + jobId))
                    .GET().build();

            HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() != 200) {
                log.warn("Could not poll Flink job {}: {}", jobId, resp.statusCode());
                continue;
            }

            JsonNode job = mapper.readTree(resp.body());
            String state = job.get("state").asText();

            log.debug("Flink job {} state: {}", jobId, state);

            switch (state) {
                case "FINISHED": {
                    // Sum write-records across all vertices for the output record count
                    long records = 0L;
                    JsonNode vertices = job.get("vertices");
                    if (vertices != null && vertices.isArray()) {
                        for (JsonNode v : vertices) {
                            if (v.has("write-records")) records += v.get("write-records").asLong();
                        }
                    }
                    log.info("Flink job {} FINISHED, write-records={}", jobId, records);
                    return records;
                }
                case "FAILED": {
                    String cause = job.has("exceptions") ? job.get("exceptions").toString() : "unknown";
                    throw new RuntimeException("Flink job FAILED: " + cause);
                }
                case "CANCELED":
                    throw new RuntimeException("Flink job was CANCELED");
                default:
                    // RUNNING, CREATED, RESTARTING — keep polling
            }
        }
        throw new RuntimeException("Flink job timed out after " + JOB_TIMEOUT.toSeconds() + "s");
    }
}
