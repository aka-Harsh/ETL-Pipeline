package com.flowforge.controller;

import com.flowforge.model.NodeType;
import com.flowforge.model.Pipeline;
import com.flowforge.repository.NodeTypeRepository;
import com.flowforge.service.PipelineService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api")
@RequiredArgsConstructor
@Slf4j
public class PipelineController {

    private final PipelineService pipelineService;
    private final NodeTypeRepository nodeTypeRepository;

    @GetMapping("/pipelines")
    public ResponseEntity<List<Pipeline>> getAllPipelines() {
        return ResponseEntity.ok(pipelineService.getAllPipelines());
    }

    @GetMapping("/pipelines/{id}")
    public ResponseEntity<Pipeline> getPipeline(@PathVariable UUID id) {
        return pipelineService.getPipeline(id)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @PostMapping("/pipelines")
    public ResponseEntity<?> createPipeline(@RequestBody Map<String, Object> request) {
        try {
            Pipeline created = pipelineService.createPipeline(request);
            return ResponseEntity.ok(created);
        } catch (Exception e) {
            log.error("Failed to create pipeline: {}", e.getMessage());
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        }
    }

    @PutMapping("/pipelines/{id}")
    public ResponseEntity<?> updatePipeline(@PathVariable UUID id, @RequestBody Map<String, Object> request) {
        try {
            return pipelineService.updatePipeline(id, request)
                    .map(ResponseEntity::ok)
                    .orElse(ResponseEntity.notFound().build());
        } catch (Exception e) {
            log.error("Failed to update pipeline {}: {}", id, e.getMessage());
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        }
    }

    @DeleteMapping("/pipelines/{id}")
    public ResponseEntity<?> deletePipeline(@PathVariable UUID id) {
        boolean deleted = pipelineService.deletePipeline(id);
        return deleted ? ResponseEntity.ok(Map.of("deleted", true)) : ResponseEntity.notFound().build();
    }

    @GetMapping("/node-types")
    public ResponseEntity<List<NodeType>> getNodeTypes() {
        return ResponseEntity.ok(nodeTypeRepository.findAll());
    }
}
