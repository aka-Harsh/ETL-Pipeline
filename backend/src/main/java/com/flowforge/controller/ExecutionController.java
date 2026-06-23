package com.flowforge.controller;

import com.flowforge.service.AuditService;
import com.flowforge.service.ExecutionOrchestrator;
import com.flowforge.service.StatusService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/pipelines")
@RequiredArgsConstructor
@Slf4j
public class ExecutionController {

    private final ExecutionOrchestrator orchestrator;
    private final StatusService statusService;
    private final AuditService auditService;

    @PostMapping("/{id}/execute")
    public ResponseEntity<?> executePipeline(@PathVariable UUID id) {
        log.info("Execute request for pipeline: {}", id);
        try {
            String executionId = UUID.randomUUID().toString();
            orchestrator.startExecution(id, executionId);
            return ResponseEntity.ok(Map.of(
                    "message", "Pipeline execution started",
                    "executionId", executionId,
                    "pipelineId", id.toString()
            ));
        } catch (Exception e) {
            log.error("Failed to start execution: {}", e.getMessage());
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        }
    }

    @GetMapping("/{id}/status")
    public ResponseEntity<Map<String, Object>> getStatus(@PathVariable String id,
                                                          @RequestParam(required = false) String executionId) {
        if (executionId != null) {
            Map<String, Object> overall = statusService.getExecutionStatus(executionId);
            Map<String, Object> nodes = statusService.getNodeStatuses(executionId);
            overall.put("nodeStatuses", nodes);
            return ResponseEntity.ok(overall);
        }
        // Return empty status if no executionId provided
        return ResponseEntity.ok(Map.of("overallStatus", "IDLE", "nodeStatuses", Map.of()));
    }

    @GetMapping("/{id}/history")
    public ResponseEntity<List<Map<String, Object>>> getHistory(@PathVariable String id) {
        return ResponseEntity.ok(auditService.getExecutionHistory(id));
    }
}
