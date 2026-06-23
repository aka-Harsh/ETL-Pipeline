package com.flowforge.worker;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flowforge.config.RabbitMQConfig;
import com.flowforge.model.TaskMessage;
import com.flowforge.rabbitmq.TaskPublisher;
import com.flowforge.service.FlinkJobSubmitter;
import com.flowforge.service.StatusService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Component
@Slf4j
public class FilterExecutor extends BaseWorker {

    private final FlinkJobSubmitter flinkJobSubmitter;

    @Value("${spring.kafka.bootstrap-servers}")
    private String kafkaBootstrap;

    @Value("${flink.enabled:true}")
    private boolean flinkEnabled;

    public FilterExecutor(TaskPublisher taskPublisher, StatusService statusService,
                          ObjectMapper objectMapper, FlinkJobSubmitter flinkJobSubmitter) {
        super(taskPublisher, statusService, objectMapper);
        this.flinkJobSubmitter = flinkJobSubmitter;
    }

    @RabbitListener(queues = RabbitMQConfig.QUEUE_FILTER)
    public void handleTask(TaskMessage task) {
        log.info("FilterExecutor: executionId={} nodeId={}", task.getExecutionId(), task.getNodeId());
        statusService.setNodeStatus(task.getExecutionId(), task.getNodeId(), "RUNNING");
        long start = System.currentTimeMillis();

        try {
            long records = execute(task);
            long duration = System.currentTimeMillis() - start;
            double rps = duration > 0 ? (records * 1000.0 / duration) : records;
            statusService.setNodeStatus(task.getExecutionId(), task.getNodeId(), "COMPLETED", records, rps);
            publishSuccess(task, records, duration);
        } catch (Exception e) {
            log.error("FilterExecutor failed: {}", e.getMessage(), e);
            statusService.setNodeError(task.getExecutionId(), task.getNodeId(), e.getMessage());
            publishFailure(task, e.getMessage());
        }
    }

    @Override
    public long execute(TaskMessage task) throws Exception {
        Map<String, Object> config = task.getConfig();
        String field = (String) config.get("field");
        String operator = (String) config.getOrDefault("operator", "==");
        String value = String.valueOf(config.get("value"));

        // Attempt Flink execution first; fall back to in-process if Flink unavailable.
        // Flink can only write to a single output topic, so skip it for fan-out nodes.
        if (flinkEnabled && task.getInputTopics() != null && !task.getInputTopics().isEmpty()
                && task.getOutputTopics() != null && task.getOutputTopics().size() == 1) {
            try {
                log.info("FilterExecutor: submitting Flink FilterJob for executionId={}", task.getExecutionId());
                long records = flinkJobSubmitter.submitFilterJob(
                        task.getInputTopics().get(0),
                        task.getOutputTopics().get(0),
                        kafkaBootstrap,
                        field, operator, value,
                        task.getExecutionId()
                );
                log.info("FilterExecutor: Flink job completed, records={}", records);
                return records;
            } catch (Exception e) {
                log.warn("FilterExecutor: Flink submission failed ({}), falling back to in-process filter", e.getMessage());
            }
        }

        // In-process fallback
        String groupId = "flowforge-filter-" + task.getExecutionId() + "-" + task.getNodeId();
        List<String> inputRecords = drainTopics(task.getInputTopics(), groupId);

        List<String> filtered = new ArrayList<>();
        for (String recordJson : inputRecords) {
            try {
                JsonNode node = objectMapper.readTree(recordJson);
                if (matches(node, field, operator, value)) {
                    filtered.add(recordJson);
                }
            } catch (Exception e) {
                log.warn("Skipping malformed record: {}", e.getMessage());
            }
        }

        log.info("Filter: {} → {} records (field={} {} {})", inputRecords.size(), filtered.size(), field, operator, value);
        publishToTopics(filtered, task.getOutputTopics());
        return filtered.size();
    }

    private boolean matches(JsonNode node, String field, String operator, String value) {
        JsonNode fieldNode = node.get(field);
        if (fieldNode == null) return false;
        String fieldValue = fieldNode.asText();
        return switch (operator) {
            case "==" -> fieldValue.equals(value);
            case "!=" -> !fieldValue.equals(value);
            case "contains" -> fieldValue.contains(value);
            case ">" -> toDouble(fieldValue) > toDouble(value);
            case "<" -> toDouble(fieldValue) < toDouble(value);
            case ">=" -> toDouble(fieldValue) >= toDouble(value);
            case "<=" -> toDouble(fieldValue) <= toDouble(value);
            default -> false;
        };
    }

    private double toDouble(String s) {
        try { return Double.parseDouble(s); } catch (Exception e) { return 0; }
    }
}
