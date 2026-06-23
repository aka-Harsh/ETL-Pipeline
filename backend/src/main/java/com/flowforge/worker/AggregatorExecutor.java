package com.flowforge.worker;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.flowforge.config.RabbitMQConfig;
import com.flowforge.model.TaskMessage;
import com.flowforge.rabbitmq.TaskPublisher;
import com.flowforge.service.FlinkJobSubmitter;
import com.flowforge.service.StatusService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.*;

@Component
@Slf4j
public class AggregatorExecutor extends BaseWorker {

    private final FlinkJobSubmitter flinkJobSubmitter;

    @Value("${spring.kafka.bootstrap-servers}")
    private String kafkaBootstrap;

    @Value("${flink.enabled:true}")
    private boolean flinkEnabled;

    public AggregatorExecutor(TaskPublisher taskPublisher, StatusService statusService,
                               ObjectMapper objectMapper, FlinkJobSubmitter flinkJobSubmitter) {
        super(taskPublisher, statusService, objectMapper);
        this.flinkJobSubmitter = flinkJobSubmitter;
    }

    @RabbitListener(queues = RabbitMQConfig.QUEUE_AGGREGATOR)
    public void handleTask(TaskMessage task) {
        log.info("AggregatorExecutor: executionId={} nodeId={}", task.getExecutionId(), task.getNodeId());
        statusService.setNodeStatus(task.getExecutionId(), task.getNodeId(), "RUNNING");
        long start = System.currentTimeMillis();
        try {
            long records = execute(task);
            long duration = System.currentTimeMillis() - start;
            statusService.setNodeStatus(task.getExecutionId(), task.getNodeId(), "COMPLETED", records,
                    duration > 0 ? records * 1000.0 / duration : records);
            publishSuccess(task, records, duration);
        } catch (Exception e) {
            log.error("AggregatorExecutor failed: {}", e.getMessage(), e);
            statusService.setNodeError(task.getExecutionId(), task.getNodeId(), e.getMessage());
            publishFailure(task, e.getMessage());
        }
    }

    @Override
    public long execute(TaskMessage task) throws Exception {
        Map<String, Object> config = task.getConfig();
        String groupBy = (String) config.get("groupBy");
        String function = (String) config.getOrDefault("function", "count");
        String field = (String) config.getOrDefault("field", "");

        // Attempt Flink execution first; fall back to in-process if Flink unavailable.
        // Flink can only write to a single output topic, so skip it for fan-out nodes.
        if (flinkEnabled && task.getInputTopics() != null && !task.getInputTopics().isEmpty()
                && task.getOutputTopics() != null && task.getOutputTopics().size() == 1) {
            try {
                log.info("AggregatorExecutor: submitting Flink AggregatorJob for executionId={}", task.getExecutionId());
                long records = flinkJobSubmitter.submitAggregatorJob(
                        task.getInputTopics().get(0),
                        task.getOutputTopics().get(0),
                        kafkaBootstrap,
                        groupBy, function, field,
                        task.getExecutionId()
                );
                log.info("AggregatorExecutor: Flink job completed, records={}", records);
                return records;
            } catch (Exception e) {
                log.warn("AggregatorExecutor: Flink submission failed ({}), falling back to in-process aggregator", e.getMessage());
            }
        }

        // In-process fallback
        String groupId = "flowforge-aggregator-" + task.getExecutionId() + "-" + task.getNodeId();
        List<String> inputRecords = drainTopics(task.getInputTopics(), groupId);

        // Group records
        Map<String, List<Double>> groups = new LinkedHashMap<>();
        for (String recordJson : inputRecords) {
            try {
                JsonNode node = objectMapper.readTree(recordJson);
                String key = node.has(groupBy) ? node.get(groupBy).asText() : "null";
                double val = node.has(field) ? node.get(field).asDouble() : 0;
                groups.computeIfAbsent(key, k -> new ArrayList<>()).add(val);
            } catch (Exception e) {
                log.warn("Skipping record in aggregator: {}", e.getMessage());
            }
        }

        // Produce aggregated results
        List<String> results = new ArrayList<>();
        for (Map.Entry<String, List<Double>> entry : groups.entrySet()) {
            ObjectNode result = objectMapper.createObjectNode();
            result.put(groupBy, entry.getKey());
            result.put("count", entry.getValue().size());
            switch (function) {
                case "sum" -> result.put(field + "_sum", entry.getValue().stream().mapToDouble(d -> d).sum());
                case "avg" -> result.put(field + "_avg",
                        entry.getValue().stream().mapToDouble(d -> d).average().orElse(0));
                default -> { /* count already set */ }
            }
            results.add(objectMapper.writeValueAsString(result));
        }

        publishToTopics(results, task.getOutputTopics());
        log.info("Aggregator: {} input records → {} groups", inputRecords.size(), results.size());
        return results.size();
    }
}
