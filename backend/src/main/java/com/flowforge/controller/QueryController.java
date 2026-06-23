package com.flowforge.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import javax.sql.DataSource;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/query")
@RequiredArgsConstructor
@Slf4j
public class QueryController {

    private final DataSource dataSource;
    private final S3Client s3Client;

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    // ─── Postgres ────────────────────────────────────────────────────────────────

    @GetMapping("/tables")
    public ResponseEntity<List<String>> getTables() {
        List<String> tables = new ArrayList<>();
        String sql = """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public'
                  AND table_type = 'BASE TABLE'
                  AND table_name NOT IN ('pipelines', 'node_types')
                ORDER BY table_name
                """;
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                tables.add(rs.getString("table_name"));
            }
        } catch (Exception e) {
            log.error("Failed to list tables: {}", e.getMessage());
            return ResponseEntity.internalServerError().body(List.of());
        }
        return ResponseEntity.ok(tables);
    }

    @GetMapping("/preview")
    public ResponseEntity<Map<String, Object>> preview(@RequestParam String table,
                                                        @RequestParam(defaultValue = "200") int limit) {
        String safeTable = table.replaceAll("[^a-zA-Z0-9_]", "");
        if (safeTable.isEmpty()) return ResponseEntity.badRequest().body(Map.of("error", "Invalid table name"));
        limit = Math.min(limit, 500);

        List<Map<String, Object>> rows = new ArrayList<>();
        List<String> columns = new ArrayList<>();
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM \"" + safeTable + "\" LIMIT " + limit)) {
            ResultSetMetaData rsMeta = rs.getMetaData();
            for (int i = 1; i <= rsMeta.getColumnCount(); i++) columns.add(rsMeta.getColumnName(i));
            while (rs.next()) {
                Map<String, Object> row = new LinkedHashMap<>();
                for (String col : columns) row.put(col, rs.getObject(col));
                rows.add(row);
            }
        } catch (Exception e) {
            log.error("Query failed for table {}: {}", safeTable, e.getMessage());
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        }
        return ResponseEntity.ok(Map.of("table", safeTable, "columns", columns, "rows", rows, "count", rows.size()));
    }

    @DeleteMapping("/table")
    public ResponseEntity<Map<String, Object>> dropTable(@RequestParam String table) {
        String safeTable = table.replaceAll("[^a-zA-Z0-9_]", "");
        if (safeTable.isEmpty() || safeTable.equals("pipelines") || safeTable.equals("node_types"))
            return ResponseEntity.badRequest().body(Map.of("error", "Cannot drop that table"));
        try (Connection conn = dataSource.getConnection(); Statement stmt = conn.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS \"" + safeTable + "\"");
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(Map.of("error", e.getMessage()));
        }
        return ResponseEntity.ok(Map.of("dropped", safeTable));
    }

    // ─── S3 ──────────────────────────────────────────────────────────────────────

    @GetMapping("/s3")
    public ResponseEntity<Map<String, Object>> listS3(
            @RequestParam(defaultValue = "flowforge-output") String bucket,
            @RequestParam(defaultValue = "") String prefix) {
        try {
            var resp = s3Client.listObjectsV2(ListObjectsV2Request.builder()
                    .bucket(bucket).prefix(prefix).maxKeys(200).build());
            var files = resp.contents().stream().map(o -> Map.of(
                    "key", (Object) o.key(), "size", o.size(), "lastModified", o.lastModified().toString()
            )).collect(Collectors.toList());
            return ResponseEntity.ok(Map.of("bucket", bucket, "prefix", prefix, "files", files));
        } catch (Exception e) {
            log.error("S3 list failed: {}", e.getMessage());
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        }
    }

    /** Returns the text content of an S3 object (capped at 50 KB). */
    @GetMapping("/s3/content")
    public ResponseEntity<Map<String, Object>> getS3Content(
            @RequestParam String bucket, @RequestParam String key) {
        try {
            ResponseInputStream<GetObjectResponse> obj = s3Client.getObject(
                    GetObjectRequest.builder().bucket(bucket).key(key).build());
            BufferedReader reader = new BufferedReader(new InputStreamReader(obj, StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder();
            char[] buf = new char[1024];
            int read;
            while ((read = reader.read(buf)) != -1) {
                sb.append(buf, 0, read);
                if (sb.length() > 51200) { sb.append("\n\n... [truncated at 50 KB] ..."); break; }
            }
            return ResponseEntity.ok(Map.of("bucket", bucket, "key", key, "content", sb.toString()));
        } catch (Exception e) {
            log.error("S3 read failed for {}/{}: {}", bucket, key, e.getMessage());
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        }
    }

    /** Deletes an S3 object. Refuses to delete files in the input bucket. */
    @DeleteMapping("/s3/object")
    public ResponseEntity<Map<String, Object>> deleteS3Object(
            @RequestParam String bucket, @RequestParam String key) {
        if ("flowforge-data".equals(bucket) && key.startsWith("input/"))
            return ResponseEntity.badRequest().body(Map.of("error", "Cannot delete source input files"));
        try {
            s3Client.deleteObject(DeleteObjectRequest.builder().bucket(bucket).key(key).build());
            log.info("Deleted s3://{}/{}", bucket, key);
            return ResponseEntity.ok(Map.of("deleted", key));
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(Map.of("error", e.getMessage()));
        }
    }

    // ─── Kafka ───────────────────────────────────────────────────────────────────

    /** Lists all non-internal Kafka topics. */
    @GetMapping("/kafka/topics")
    public ResponseEntity<List<String>> listKafkaTopics() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        try (AdminClient admin = AdminClient.create(props)) {
            List<String> topics = admin.listTopics().names().get().stream()
                    .filter(t -> !t.startsWith("__"))
                    .sorted()
                    .collect(Collectors.toList());
            return ResponseEntity.ok(topics);
        } catch (Exception e) {
            log.error("Kafka topic list failed: {}", e.getMessage());
            return ResponseEntity.internalServerError().build();
        }
    }

    /** Reads up to 100 messages from the beginning of a Kafka topic. */
    @GetMapping("/kafka/messages")
    public ResponseEntity<Map<String, Object>> getKafkaMessages(
            @RequestParam String topic,
            @RequestParam(defaultValue = "100") int limit) {
        limit = Math.min(limit, 200);
        List<Map<String, Object>> messages = new ArrayList<>();

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "flowforge-viewer-" + UUID.randomUUID());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, String.valueOf(limit));

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(topic));
            // Wait for assignment
            int waits = 0;
            while (consumer.assignment().isEmpty() && waits++ < 15) {
                consumer.poll(Duration.ofMillis(500));
            }
            consumer.seekToBeginning(consumer.assignment());

            int emptyPolls = 0;
            while (messages.size() < limit && emptyPolls < 3) {
                ConsumerRecords<String, String> batch = consumer.poll(Duration.ofMillis(1500));
                if (batch.isEmpty()) {
                    emptyPolls++;
                } else {
                    emptyPolls = 0;
                    for (var record : batch) {
                        if (messages.size() >= limit) break;
                        messages.add(Map.of(
                                "offset", record.offset(),
                                "partition", record.partition(),
                                "key", record.key() != null ? record.key() : "",
                                "value", record.value() != null ? record.value() : ""
                        ));
                    }
                }
            }
        } catch (Exception e) {
            log.error("Kafka read failed for topic {}: {}", topic, e.getMessage());
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        }

        return ResponseEntity.ok(Map.of("topic", topic, "messages", messages, "count", messages.size()));
    }

    /** Deletes a Kafka topic. Refuses to delete internal topics. */
    @DeleteMapping("/kafka/topic")
    public ResponseEntity<Map<String, Object>> deleteTopic(@RequestParam String topic) {
        if (topic.startsWith("__"))
            return ResponseEntity.badRequest().body(Map.of("error", "Cannot delete internal topic"));
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        try (AdminClient admin = AdminClient.create(props)) {
            admin.deleteTopics(List.of(topic)).all().get();
            return ResponseEntity.ok(Map.of("deleted", topic));
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(Map.of("error", e.getMessage()));
        }
    }
}
