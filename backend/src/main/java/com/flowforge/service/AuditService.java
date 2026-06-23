package com.flowforge.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flowforge.model.ExecutionRecord;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.time.Instant;
import java.util.*;

@Service
@RequiredArgsConstructor
@Slf4j
public class AuditService {

    private final DynamoDbClient dynamoDbClient;
    private final ObjectMapper objectMapper;

    @Value("${aws.dynamodb.table-name:FlowForgeExecutions}")
    private String tableName;

    public void recordExecutionStarted(String executionId, String pipelineId, String triggeredBy) {
        try {
            Map<String, AttributeValue> item = new HashMap<>();
            item.put("pipelineId", AttributeValue.fromS(pipelineId));
            item.put("executionTimestamp", AttributeValue.fromS(Instant.now().toString()));
            item.put("executionId", AttributeValue.fromS(executionId));
            item.put("status", AttributeValue.fromS("STARTED"));
            item.put("triggeredBy", AttributeValue.fromS(triggeredBy));

            dynamoDbClient.putItem(PutItemRequest.builder()
                    .tableName(tableName)
                    .item(item)
                    .build());
            log.info("Recorded execution started: {}", executionId);
        } catch (Exception e) {
            log.warn("Failed to record execution start in DynamoDB: {}", e.getMessage());
        }
    }

    public void recordExecutionCompleted(String executionId, String pipelineId,
                                          String status, long durationMs,
                                          long totalRecords, Map<String, Object> nodeResults) {
        try {
            Map<String, AttributeValue> item = new HashMap<>();
            item.put("pipelineId", AttributeValue.fromS(pipelineId));
            item.put("executionTimestamp", AttributeValue.fromS(Instant.now().toString()));
            item.put("executionId", AttributeValue.fromS(executionId));
            item.put("status", AttributeValue.fromS(status));
            item.put("durationMs", AttributeValue.fromN(String.valueOf(durationMs)));
            item.put("totalRecordsProcessed", AttributeValue.fromN(String.valueOf(totalRecords)));
            item.put("nodeResults", AttributeValue.fromS(objectMapper.writeValueAsString(nodeResults)));

            dynamoDbClient.putItem(PutItemRequest.builder()
                    .tableName(tableName)
                    .item(item)
                    .build());
            log.info("Recorded execution completed: {} status={}", executionId, status);
        } catch (Exception e) {
            log.warn("Failed to record execution completion in DynamoDB: {}", e.getMessage());
        }
    }

    public List<Map<String, Object>> getExecutionHistory(String pipelineId) {
        try {
            QueryResponse response = dynamoDbClient.query(QueryRequest.builder()
                    .tableName(tableName)
                    .keyConditionExpression("pipelineId = :pk")
                    .expressionAttributeValues(Map.of(
                            ":pk", AttributeValue.fromS(pipelineId)))
                    .scanIndexForward(false)
                    .limit(20)
                    .build());

            List<Map<String, Object>> history = new ArrayList<>();
            for (Map<String, AttributeValue> item : response.items()) {
                Map<String, Object> record = new HashMap<>();
                item.forEach((k, v) -> {
                    if (v.s() != null) record.put(k, v.s());
                    else if (v.n() != null) record.put(k, v.n());
                    else if (v.bool() != null) record.put(k, v.bool());
                });
                history.add(record);
            }
            return history;
        } catch (Exception e) {
            log.warn("Failed to query DynamoDB history: {}", e.getMessage());
            return List.of();
        }
    }
}
