package com.flowforge.rabbitmq;

import com.flowforge.config.RabbitMQConfig;
import com.flowforge.model.TaskMessage;
import com.flowforge.service.StatusService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class DLQHandler {

    private final StatusService statusService;

    @RabbitListener(queues = RabbitMQConfig.QUEUE_DLQ)
    public void handleDeadLetter(TaskMessage task) {
        if (task == null) {
            log.warn("Received null message in DLQ");
            return;
        }
        log.error("Dead letter received: executionId={} nodeId={} nodeType={}",
                task.getExecutionId(), task.getNodeId(), task.getNodeType());
        statusService.setNodeError(task.getExecutionId(), task.getNodeId(),
                "Node failed after maximum retries — moved to dead letter queue");
        statusService.setOverallStatus(task.getExecutionId(), task.getPipelineId(), "FAILED");
    }
}
