package com.flowforge.rabbitmq;

import com.flowforge.config.RabbitMQConfig;
import com.flowforge.model.CompletionEvent;
import com.flowforge.model.TaskMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class TaskPublisher {

    private final RabbitTemplate rabbitTemplate;

    /**
     * Publishes a node task to the appropriate RabbitMQ queue via the tasks exchange.
     */
    public void publishTask(TaskMessage task) {
        String routingKey = nodeTypeToRoutingKey(task.getNodeType());
        log.info("Publishing task: executionId={} nodeId={} nodeType={} → routingKey={}",
                task.getExecutionId(), task.getNodeId(), task.getNodeType(), routingKey);
        rabbitTemplate.convertAndSend(RabbitMQConfig.TASKS_EXCHANGE, routingKey, task);
    }

    /**
     * Publishes a completion event to the events exchange.
     */
    public void publishCompletion(CompletionEvent event) {
        String routingKey = event.isSuccess() ? RabbitMQConfig.RK_COMPLETED : RabbitMQConfig.RK_FAILED;
        log.info("Publishing {} event: executionId={} nodeId={} records={}",
                routingKey, event.getExecutionId(), event.getNodeId(), event.getRecordsProcessed());
        rabbitTemplate.convertAndSend(RabbitMQConfig.EVENTS_EXCHANGE, routingKey, event);
    }

    private String nodeTypeToRoutingKey(String nodeType) {
        return switch (nodeType.toUpperCase()) {
            case "S3_SOURCE"     -> RabbitMQConfig.RK_S3_SOURCE;
            case "KAFKA_SOURCE"  -> RabbitMQConfig.RK_KAFKA_SOURCE;
            case "FILTER"        -> RabbitMQConfig.RK_FILTER;
            case "FIELD_MAPPER"  -> RabbitMQConfig.RK_MAPPER;
            case "AGGREGATOR"    -> RabbitMQConfig.RK_AGGREGATOR;
            case "POSTGRES_SINK" -> RabbitMQConfig.RK_PG_SINK;
            case "S3_SINK"       -> RabbitMQConfig.RK_S3_SINK;
            case "KAFKA_SINK"    -> RabbitMQConfig.RK_KAFKA_SINK;
            default -> throw new IllegalArgumentException("Unknown node type: " + nodeType);
        };
    }
}
