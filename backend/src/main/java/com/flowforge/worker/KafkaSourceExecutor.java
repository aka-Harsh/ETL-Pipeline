package com.flowforge.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flowforge.config.RabbitMQConfig;
import com.flowforge.model.TaskMessage;
import com.flowforge.rabbitmq.TaskPublisher;
import com.flowforge.service.StatusService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

@Component
@Slf4j
public class KafkaSourceExecutor extends BaseWorker {

    public KafkaSourceExecutor(TaskPublisher taskPublisher, StatusService statusService, ObjectMapper objectMapper) {
        super(taskPublisher, statusService, objectMapper);
    }

    @RabbitListener(queues = RabbitMQConfig.QUEUE_KAFKA_SOURCE)
    public void handleTask(TaskMessage task) {
        log.info("KafkaSourceExecutor: executionId={} nodeId={}", task.getExecutionId(), task.getNodeId());
        statusService.setNodeStatus(task.getExecutionId(), task.getNodeId(), "RUNNING");
        long start = System.currentTimeMillis();
        try {
            long records = execute(task);
            long duration = System.currentTimeMillis() - start;
            statusService.setNodeStatus(task.getExecutionId(), task.getNodeId(), "COMPLETED", records,
                    duration > 0 ? records * 1000.0 / duration : records);
            publishSuccess(task, records, duration);
        } catch (Exception e) {
            log.error("KafkaSourceExecutor failed: {}", e.getMessage(), e);
            statusService.setNodeError(task.getExecutionId(), task.getNodeId(), e.getMessage());
            publishFailure(task, e.getMessage());
        }
    }

    @Override
    public long execute(TaskMessage task) throws Exception {
        Map<String, Object> config = task.getConfig();
        String sourceTopic = (String) config.get("topic");
        String consumerGroup = (String) config.getOrDefault("consumerGroup",
                "flowforge-ksource-" + task.getExecutionId());

        List<String> records = drainTopics(List.of(sourceTopic), consumerGroup);
        publishToTopics(records, task.getOutputTopics());
        log.info("KafkaSource: read {} records from topic {} → {} output topics",
                records.size(), sourceTopic, task.getOutputTopics().size());
        return records.size();
    }
}
