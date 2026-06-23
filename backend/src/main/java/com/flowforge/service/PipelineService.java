package com.flowforge.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flowforge.model.Pipeline;
import com.flowforge.model.PipelineDefinition;
import com.flowforge.repository.PipelineRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.*;

@Service
@RequiredArgsConstructor
@Slf4j
public class PipelineService {

    private final PipelineRepository pipelineRepository;
    private final ObjectMapper objectMapper;

    public List<Pipeline> getAllPipelines() {
        return pipelineRepository.findByIsActiveTrueOrderByCreatedAtDesc();
    }

    public Optional<Pipeline> getPipeline(UUID id) {
        return pipelineRepository.findById(id);
    }

    @Transactional
    public Pipeline createPipeline(Map<String, Object> request) throws Exception {
        Pipeline pipeline = new Pipeline();
        pipeline.setName((String) request.get("name"));
        pipeline.setDescription((String) request.get("description"));

        Object definition = request.get("definition");
        pipeline.setDefinition(objectMapper.writeValueAsString(definition));

        return pipelineRepository.save(pipeline);
    }

    @Transactional
    public Optional<Pipeline> updatePipeline(UUID id, Map<String, Object> request) throws Exception {
        return pipelineRepository.findById(id).map(pipeline -> {
            if (request.containsKey("name")) pipeline.setName((String) request.get("name"));
            if (request.containsKey("description")) pipeline.setDescription((String) request.get("description"));
            if (request.containsKey("definition")) {
                try {
                    pipeline.setDefinition(objectMapper.writeValueAsString(request.get("definition")));
                } catch (Exception e) {
                    throw new RuntimeException("Failed to serialize definition", e);
                }
            }
            return pipelineRepository.save(pipeline);
        });
    }

    @Transactional
    public boolean deletePipeline(UUID id) {
        return pipelineRepository.findById(id).map(pipeline -> {
            pipeline.setActive(false);
            pipelineRepository.save(pipeline);
            return true;
        }).orElse(false);
    }

    /**
     * Parses the pipeline definition JSON and validates it as a DAG.
     * Returns the parsed definition if valid, throws if invalid.
     */
    public PipelineDefinition parsAndValidateDefinition(String definitionJson) throws Exception {
        PipelineDefinition def = objectMapper.readValue(definitionJson, PipelineDefinition.class);
        validateDAG(def);
        return def;
    }

    /**
     * Validates that the pipeline is a valid DAG (no cycles) using DFS.
     */
    private void validateDAG(PipelineDefinition def) {
        if (def.getNodes() == null || def.getNodes().isEmpty()) {
            throw new IllegalArgumentException("Pipeline must have at least one node");
        }
        if (def.getEdges() == null || def.getEdges().isEmpty()) {
            throw new IllegalArgumentException("Pipeline must have at least one edge");
        }

        // Build adjacency list
        Map<String, List<String>> adj = new HashMap<>();
        for (PipelineDefinition.NodeDefinition node : def.getNodes()) {
            adj.put(node.getId(), new ArrayList<>());
        }
        for (PipelineDefinition.EdgeDefinition edge : def.getEdges()) {
            adj.computeIfAbsent(edge.getSource(), k -> new ArrayList<>()).add(edge.getTarget());
        }

        // DFS cycle detection
        Set<String> visited = new HashSet<>();
        Set<String> inStack = new HashSet<>();
        for (String nodeId : adj.keySet()) {
            if (!visited.contains(nodeId)) {
                if (hasCycle(nodeId, adj, visited, inStack)) {
                    throw new IllegalArgumentException("Pipeline contains a cycle — DAG validation failed");
                }
            }
        }
    }

    private boolean hasCycle(String node, Map<String, List<String>> adj,
                              Set<String> visited, Set<String> inStack) {
        visited.add(node);
        inStack.add(node);
        for (String neighbour : adj.getOrDefault(node, List.of())) {
            if (!visited.contains(neighbour)) {
                if (hasCycle(neighbour, adj, visited, inStack)) return true;
            } else if (inStack.contains(neighbour)) {
                return true;
            }
        }
        inStack.remove(node);
        return false;
    }
}
