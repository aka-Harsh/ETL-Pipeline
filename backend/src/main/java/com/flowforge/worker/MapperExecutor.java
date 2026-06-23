package com.flowforge.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.flowforge.config.RabbitMQConfig;
import com.flowforge.model.TaskMessage;
import com.flowforge.rabbitmq.TaskPublisher;
import com.flowforge.service.StatusService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import java.util.*;

@Component
@Slf4j
public class MapperExecutor extends BaseWorker {

    public MapperExecutor(TaskPublisher taskPublisher, StatusService statusService, ObjectMapper objectMapper) {
        super(taskPublisher, statusService, objectMapper);
    }

    @RabbitListener(queues = RabbitMQConfig.QUEUE_MAPPER)
    public void handleTask(TaskMessage task) {
        log.info("MapperExecutor: executionId={} nodeId={}", task.getExecutionId(), task.getNodeId());
        statusService.setNodeStatus(task.getExecutionId(), task.getNodeId(), "RUNNING");
        long start = System.currentTimeMillis();
        try {
            long records = execute(task);
            long duration = System.currentTimeMillis() - start;
            statusService.setNodeStatus(task.getExecutionId(), task.getNodeId(), "COMPLETED", records,
                    duration > 0 ? records * 1000.0 / duration : records);
            publishSuccess(task, records, duration);
        } catch (Exception e) {
            log.error("MapperExecutor failed: {}", e.getMessage(), e);
            statusService.setNodeError(task.getExecutionId(), task.getNodeId(), e.getMessage());
            publishFailure(task, e.getMessage());
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public long execute(TaskMessage task) throws Exception {
        Map<String, Object> config = task.getConfig();
        List<Map<String, Object>> mappings =
                (List<Map<String, Object>>) config.getOrDefault("mappings", List.of());

        String groupId = "flowforge-mapper-" + task.getExecutionId() + "-" + task.getNodeId();
        List<String> inputRecords = drainTopics(task.getInputTopics(), groupId);
        List<String> mapped = new ArrayList<>();

        for (String recordJson : inputRecords) {
            try {
                ObjectNode node = (ObjectNode) objectMapper.readTree(recordJson);
                // Start with a copy of ALL input fields so non-mapped fields are preserved
                ObjectNode result = node.deepCopy();
                for (Map<String, Object> mapping : mappings) {
                    // Accept multiple key names the AI might use for "from" and "to"
                    String from = firstString(mapping, "from", "source", "input", "field");
                    String to   = firstString(mapping, "to", "target", "output", "rename", "as");
                    if (from != null && to != null && !from.isBlank() && !to.isBlank()
                            && result.has(from)) {
                        result.set(to, result.get(from));   // add renamed field
                        if (!from.equals(to)) result.remove(from); // remove old name
                    }
                }
                mapped.add(objectMapper.writeValueAsString(result));
            } catch (Exception e) {
                log.warn("Skipping malformed record in mapper: {}", e.getMessage());
            }
        }

        publishToTopics(mapped, task.getOutputTopics());
        return mapped.size();
    }

    /** Returns the value of the first matching key found in the map, cast to String. */
    private String firstString(Map<String, Object> map, String... keys) {
        for (String key : keys) {
            Object v = map.get(key);
            if (v instanceof String s && !s.isBlank()) return s;
        }
        return null;
    }
}
