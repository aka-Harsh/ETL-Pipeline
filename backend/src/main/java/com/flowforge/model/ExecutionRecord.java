package com.flowforge.model;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * Represents a pipeline execution record stored in DynamoDB.
 */
@Data
@NoArgsConstructor
public class ExecutionRecord {
    private String pipelineId;
    private String executionTimestamp;
    private String executionId;
    private String status;
    private Long durationMs;
    private Long totalRecordsProcessed;
    private Map<String, Object> nodeResults;
    private String triggeredBy;
    private String errorDetails;
}
