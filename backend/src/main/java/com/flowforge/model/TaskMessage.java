package com.flowforge.model;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

/**
 * Message published to RabbitMQ to instruct a worker to execute a pipeline node.
 */
@Data
@NoArgsConstructor
public class TaskMessage {
    private String executionId;
    private String pipelineId;
    private String nodeId;
    private String nodeType;
    private Map<String, Object> config;
    /** Kafka topics this node should write output records to */
    private List<String> outputTopics;
    /** Kafka topics this node should read input records from */
    private List<String> inputTopics;
}
