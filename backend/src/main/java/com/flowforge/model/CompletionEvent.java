package com.flowforge.model;

import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Event published to RabbitMQ when a node finishes executing (success or failure).
 */
@Data
@NoArgsConstructor
public class CompletionEvent {
    private String executionId;
    private String pipelineId;
    private String nodeId;
    private String nodeType;
    private boolean success;
    private String errorMessage;
    private long recordsProcessed;
    private long durationMs;
}
