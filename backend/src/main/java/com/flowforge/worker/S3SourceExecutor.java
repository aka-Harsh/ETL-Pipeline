package com.flowforge.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flowforge.config.RabbitMQConfig;
import com.flowforge.model.TaskMessage;
import com.flowforge.rabbitmq.TaskPublisher;
import com.flowforge.service.StatusService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.*;

@Component
@Slf4j
public class S3SourceExecutor extends BaseWorker {

    private final S3Client s3Client;

    public S3SourceExecutor(TaskPublisher taskPublisher, StatusService statusService,
                             ObjectMapper objectMapper, S3Client s3Client) {
        super(taskPublisher, statusService, objectMapper);
        this.s3Client = s3Client;
    }

    @RabbitListener(queues = RabbitMQConfig.QUEUE_S3_SOURCE)
    public void handleTask(TaskMessage task) {
        log.info("S3SourceExecutor: executionId={} nodeId={}", task.getExecutionId(), task.getNodeId());
        statusService.setNodeStatus(task.getExecutionId(), task.getNodeId(), "RUNNING");
        long start = System.currentTimeMillis();

        try {
            long records = execute(task);
            long duration = System.currentTimeMillis() - start;
            double rps = duration > 0 ? (records * 1000.0 / duration) : records;
            statusService.setNodeStatus(task.getExecutionId(), task.getNodeId(), "COMPLETED", records, rps);
            publishSuccess(task, records, duration);
        } catch (Exception e) {
            log.error("S3SourceExecutor failed: {}", e.getMessage(), e);
            statusService.setNodeError(task.getExecutionId(), task.getNodeId(), e.getMessage());
            publishFailure(task, e.getMessage());
        }
    }

    @Override
    public long execute(TaskMessage task) throws Exception {
        Map<String, Object> config = task.getConfig();
        String bucket = (String) config.get("bucket");
        String key = (String) config.get("key");
        String format = (String) config.getOrDefault("format", "csv");
        boolean hasHeader = Boolean.parseBoolean(String.valueOf(config.getOrDefault("hasHeader", "true")));

        log.info("Reading s3://{}/{} format={}", bucket, key, format);

        ResponseInputStream<GetObjectResponse> s3Object = s3Client.getObject(
                GetObjectRequest.builder().bucket(bucket).key(key).build());

        List<String> records = new ArrayList<>();

        if ("csv".equalsIgnoreCase(format)) {
            records = parseCsv(s3Object, hasHeader);
        } else {
            // JSON: each line is a record, or it's a JSON array
            BufferedReader reader = new BufferedReader(new InputStreamReader(s3Object, StandardCharsets.UTF_8));
            String content = reader.lines().collect(java.util.stream.Collectors.joining("\n"));
            if (content.trim().startsWith("[")) {
                // JSON array
                List<?> list = objectMapper.readValue(content, List.class);
                for (Object item : list) {
                    records.add(objectMapper.writeValueAsString(item));
                }
            } else {
                // NDJSON
                for (String line : content.split("\n")) {
                    if (!line.trim().isEmpty()) records.add(line.trim());
                }
            }
        }

        log.info("Read {} records from S3, publishing to {} topics", records.size(), task.getOutputTopics().size());
        publishToTopics(records, task.getOutputTopics());
        return records.size();
    }

    private List<String> parseCsv(ResponseInputStream<GetObjectResponse> stream, boolean hasHeader) throws Exception {
        List<String> records = new ArrayList<>();
        BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));

        CSVFormat format = hasHeader
                ? CSVFormat.DEFAULT.withFirstRecordAsHeader().withIgnoreHeaderCase().withTrim()
                : CSVFormat.DEFAULT.withTrim();

        try (CSVParser parser = new CSVParser(reader, format)) {
            for (CSVRecord csvRecord : parser) {
                Map<String, String> row = new LinkedHashMap<>();
                if (hasHeader) {
                    parser.getHeaderMap().forEach((col, idx) -> row.put(col, csvRecord.get(col)));
                } else {
                    for (int i = 0; i < csvRecord.size(); i++) {
                        row.put("col" + i, csvRecord.get(i));
                    }
                }
                records.add(objectMapper.writeValueAsString(row));
            }
        }
        return records;
    }
}
