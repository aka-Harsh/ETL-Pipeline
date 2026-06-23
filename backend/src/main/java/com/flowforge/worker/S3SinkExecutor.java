package com.flowforge.worker;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flowforge.config.RabbitMQConfig;
import com.flowforge.model.TaskMessage;
import com.flowforge.rabbitmq.TaskPublisher;
import com.flowforge.service.StatusService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;

@Component
@Slf4j
public class S3SinkExecutor extends BaseWorker {

    private final S3Client s3Client;

    public S3SinkExecutor(TaskPublisher taskPublisher, StatusService statusService,
                           ObjectMapper objectMapper, S3Client s3Client) {
        super(taskPublisher, statusService, objectMapper);
        this.s3Client = s3Client;
    }

    @RabbitListener(queues = RabbitMQConfig.QUEUE_S3_SINK)
    public void handleTask(TaskMessage task) {
        log.info("S3SinkExecutor: executionId={} nodeId={}", task.getExecutionId(), task.getNodeId());
        statusService.setNodeStatus(task.getExecutionId(), task.getNodeId(), "RUNNING");
        long start = System.currentTimeMillis();
        try {
            long records = execute(task);
            long duration = System.currentTimeMillis() - start;
            statusService.setNodeStatus(task.getExecutionId(), task.getNodeId(), "COMPLETED", records,
                    duration > 0 ? records * 1000.0 / duration : records);
            publishSuccess(task, records, duration);
        } catch (Exception e) {
            log.error("S3SinkExecutor failed: {}", e.getMessage(), e);
            statusService.setNodeError(task.getExecutionId(), task.getNodeId(), e.getMessage());
            publishFailure(task, e.getMessage());
        }
    }

    @Override
    public long execute(TaskMessage task) throws Exception {
        Map<String, Object> config = task.getConfig();
        String bucket = (String) config.getOrDefault("bucket", "flowforge-output");
        String keyPrefix = (String) config.getOrDefault("keyPrefix", "output/");
        String format = (String) config.getOrDefault("format", "json");

        String groupId = "flowforge-s3sink-" + task.getExecutionId() + "-" + task.getNodeId();
        List<String> inputRecords = drainTopics(task.getInputTopics(), groupId);

        if (inputRecords.isEmpty()) {
            log.info("No records to write to S3");
            return 0;
        }

        ensureBucketExists(bucket);

        String outputKey = keyPrefix + task.getExecutionId() + "-" + task.getNodeId()
                + "-" + Instant.now().getEpochSecond() + "." + format;
        String content;

        if ("csv".equalsIgnoreCase(format)) {
            content = toCsv(inputRecords);
        } else {
            content = "[" + String.join(",", inputRecords) + "]";
        }

        byte[] bytes = content.getBytes(StandardCharsets.UTF_8);
        s3Client.putObject(
                PutObjectRequest.builder().bucket(bucket).key(outputKey).build(),
                RequestBody.fromBytes(bytes));

        log.info("Wrote {} records to s3://{}/{}", inputRecords.size(), bucket, outputKey);
        return inputRecords.size();
    }

    private void ensureBucketExists(String bucket) {
        try {
            s3Client.headBucket(HeadBucketRequest.builder().bucket(bucket).build());
        } catch (S3Exception e) {
            try {
                s3Client.createBucket(CreateBucketRequest.builder().bucket(bucket).build());
                log.info("Auto-created S3 bucket: {}", bucket);
            } catch (Exception ex) {
                log.warn("Could not create S3 bucket {}: {}", bucket, ex.getMessage());
            }
        }
    }

    private String toCsv(List<String> records) throws Exception {
        if (records.isEmpty()) return "";
        JsonNode first = objectMapper.readTree(records.get(0));
        List<String> headers = new ArrayList<>();
        first.fieldNames().forEachRemaining(headers::add);

        StringBuilder sb = new StringBuilder();
        sb.append(String.join(",", headers)).append("\n");
        for (String recordJson : records) {
            JsonNode node = objectMapper.readTree(recordJson);
            List<String> values = new ArrayList<>();
            for (String h : headers) {
                JsonNode val = node.get(h);
                values.add(val != null ? "\"" + val.asText().replace("\"", "\"\"") + "\"" : "");
            }
            sb.append(String.join(",", values)).append("\n");
        }
        return sb.toString();
    }
}
