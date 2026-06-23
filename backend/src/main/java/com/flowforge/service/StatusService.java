package com.flowforge.service;

import com.flowforge.config.RedisConfig;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@Service
@RequiredArgsConstructor
@Slf4j
public class StatusService {

    private final StringRedisTemplate redisTemplate;
    private final SimpMessagingTemplate messagingTemplate;

    public void setNodeStatus(String executionId, String nodeId, String status) {
        String key = RedisConfig.nodeStatusKey(executionId, nodeId);
        redisTemplate.opsForHash().put(key, "status", status);
        if ("RUNNING".equals(status)) {
            redisTemplate.opsForHash().put(key, "startedAt", String.valueOf(System.currentTimeMillis()));
        }
        pushStatusUpdate(executionId, nodeId, status, 0, 0);
    }

    public void setNodeStatus(String executionId, String nodeId, String status,
                               long recordsProcessed, double recordsPerSecond) {
        String key = RedisConfig.nodeStatusKey(executionId, nodeId);
        redisTemplate.opsForHash().put(key, "status", status);
        redisTemplate.opsForHash().put(key, "recordsProcessed", String.valueOf(recordsProcessed));
        redisTemplate.opsForHash().put(key, "recordsPerSecond", String.format("%.1f", recordsPerSecond));
        if ("FAILED".equals(status) || "COMPLETED".equals(status)) {
            redisTemplate.opsForHash().put(key, "completedAt", String.valueOf(System.currentTimeMillis()));
        }
        pushStatusUpdate(executionId, nodeId, status, recordsProcessed, recordsPerSecond);
    }

    public void setNodeError(String executionId, String nodeId, String error) {
        String key = RedisConfig.nodeStatusKey(executionId, nodeId);
        redisTemplate.opsForHash().put(key, "status", "FAILED");
        redisTemplate.opsForHash().put(key, "error", error);
        pushStatusUpdate(executionId, nodeId, "FAILED", 0, 0);
    }

    public void setOverallStatus(String executionId, String pipelineId, String status) {
        String key = RedisConfig.executionStatusKey(executionId);
        redisTemplate.opsForHash().put(key, "pipelineId", pipelineId);
        redisTemplate.opsForHash().put(key, "overallStatus", status);
        if ("STARTED".equals(status) || "RUNNING".equals(status)) {
            redisTemplate.opsForHash().put(key, "startedAt", String.valueOf(System.currentTimeMillis()));
        }
        if ("COMPLETED".equals(status) || "FAILED".equals(status)) {
            redisTemplate.opsForHash().put(key, "completedAt", String.valueOf(System.currentTimeMillis()));
        }
    }

    public Map<String, Object> getExecutionStatus(String executionId) {
        Map<String, Object> result = new HashMap<>();

        // Overall status
        String overallKey = RedisConfig.executionStatusKey(executionId);
        Map<Object, Object> overall = redisTemplate.opsForHash().entries(overallKey);
        result.put("executionId", executionId);
        result.put("overallStatus", overall.getOrDefault("overallStatus", "UNKNOWN"));
        result.put("startedAt", overall.get("startedAt"));
        result.put("completedAt", overall.get("completedAt"));

        return result;
    }

    public Map<String, Object> getNodeStatuses(String executionId) {
        // Return all node statuses — caller knows which nodeIds to look up
        // We return the raw data keyed by nodeId prefix scanning
        Map<String, Object> result = new HashMap<>();
        Set<String> keys = redisTemplate.keys("pipeline:" + executionId + ":node:*");
        if (keys != null) {
            for (String key : keys) {
                String nodeId = key.substring(("pipeline:" + executionId + ":node:").length());
                Map<Object, Object> nodeData = redisTemplate.opsForHash().entries(key);
                result.put(nodeId, nodeData);
            }
        }
        return result;
    }

    private void pushStatusUpdate(String executionId, String nodeId, String status,
                                   long recordsProcessed, double recordsPerSecond) {
        try {
            Map<String, Object> update = Map.of(
                    "executionId", executionId,
                    "nodeId", nodeId,
                    "status", status,
                    "recordsProcessed", recordsProcessed,
                    "recordsPerSecond", recordsPerSecond
            );
            messagingTemplate.convertAndSend("/topic/pipeline-status/" + executionId, update);
        } catch (Exception e) {
            log.debug("Could not push WebSocket update: {}", e.getMessage());
        }
    }
}
