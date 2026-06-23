package com.flowforge.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flowforge.config.KafkaConfig;
import com.flowforge.config.RabbitMQConfig;
import com.flowforge.model.CompletionEvent;
import com.flowforge.model.Pipeline;
import com.flowforge.model.PipelineDefinition;
import com.flowforge.model.TaskMessage;
import com.flowforge.rabbitmq.TaskPublisher;
import com.flowforge.repository.PipelineRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
@Slf4j
public class ExecutionOrchestrator {

    private final PipelineRepository pipelineRepository;
    private final PipelineService pipelineService;
    private final TaskPublisher taskPublisher;
    private final StatusService statusService;
    private final AuditService auditService;
    private final ObjectMapper objectMapper;
    private final StringRedisTemplate redisTemplate;

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    /**
     * In-memory store of active pipeline definitions keyed by executionId.
     * Used by the TaskConsumer to determine downstream nodes on completion.
     */
    private final ConcurrentHashMap<String, PipelineDefinition> activeExecutions = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, String> executionToPipeline = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Map<String, String>> nodeStatuses = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Long> executionStartTimes = new ConcurrentHashMap<>();

    /**
     * Begins execution of a pipeline. Called from ExecutionController.
     */
    @Async
    public void startExecution(UUID pipelineId, String executionId) {
        Pipeline pipeline = pipelineRepository.findById(pipelineId)
                .orElseThrow(() -> new IllegalArgumentException("Pipeline not found: " + pipelineId));

        String pidStr = pipelineId.toString();

        log.info("Starting execution {} for pipeline {}", executionId, pipelineId);

        try {
            // Parse and validate the pipeline definition
            PipelineDefinition def = pipelineService.parsAndValidateDefinition(pipeline.getDefinition());

            // Record in DynamoDB
            auditService.recordExecutionStarted(executionId, pidStr, "manual");

            // Store in memory for the completion handler
            activeExecutions.put(executionId, def);
            executionToPipeline.put(executionId, pidStr);
            executionStartTimes.put(executionId, System.currentTimeMillis());

            // Initialize node statuses in Redis
            Map<String, String> statuses = new ConcurrentHashMap<>();
            for (PipelineDefinition.NodeDefinition node : def.getNodes()) {
                statusService.setNodeStatus(executionId, node.getId(), "WAITING");
                statuses.put(node.getId(), "WAITING");
            }
            nodeStatuses.put(executionId, statuses);
            statusService.setOverallStatus(executionId, pidStr, "RUNNING");

            // Create Kafka topics for each edge
            createKafkaTopics(executionId, def);

            // Build incoming-edge count per node
            Map<String, Set<String>> incomingEdges = buildIncomingEdges(def);
            Map<String, List<String>> outgoingEdges = buildOutgoingEdges(def);

            // Find root nodes (no incoming edges) and publish START tasks
            List<PipelineDefinition.NodeDefinition> rootNodes = def.getNodes().stream()
                    .filter(n -> !incomingEdges.containsKey(n.getId()) || incomingEdges.get(n.getId()).isEmpty())
                    .collect(Collectors.toList());

            if (rootNodes.isEmpty()) {
                throw new IllegalStateException("No root nodes found in pipeline");
            }

            for (PipelineDefinition.NodeDefinition node : rootNodes) {
                TaskMessage task = buildTaskMessage(executionId, pidStr, node, def, incomingEdges, outgoingEdges);
                taskPublisher.publishTask(task);
                statusService.setNodeStatus(executionId, node.getId(), "WAITING");
            }

        } catch (Exception e) {
            log.error("Failed to start execution {}: {}", executionId, e.getMessage(), e);
            statusService.setOverallStatus(executionId, pidStr, "FAILED");
            auditService.recordExecutionCompleted(executionId, pidStr, "FAILED", 0, 0, Map.of());
        }
    }

    /**
     * Called when a node completes. Determines which downstream nodes are now ready.
     */
    @RabbitListener(queues = {RabbitMQConfig.QUEUE_EVENTS_COMPLETED, RabbitMQConfig.QUEUE_EVENTS_FAILED})
    public void onNodeCompletion(CompletionEvent event) {
        String executionId = event.getExecutionId();
        String nodeId = event.getNodeId();
        String pidStr = event.getPipelineId();

        log.info("Received completion event: executionId={} nodeId={} success={}",
                executionId, nodeId, event.isSuccess());

        PipelineDefinition def = activeExecutions.get(executionId);
        if (def == null) {
            log.warn("No active execution found for executionId: {}", executionId);
            return;
        }

        // Update node status
        Map<String, String> statuses = nodeStatuses.computeIfAbsent(executionId, k -> new ConcurrentHashMap<>());
        String newStatus = event.isSuccess() ? "COMPLETED" : "FAILED";
        statuses.put(nodeId, newStatus);
        statusService.setNodeStatus(executionId, nodeId, newStatus, event.getRecordsProcessed(), 0);

        if (!event.isSuccess()) {
            statusService.setOverallStatus(executionId, pidStr, "FAILED");
            finishExecution(executionId, pidStr, "FAILED", def, statuses);
            return;
        }

        // Check if all nodes are done
        long completedCount = statuses.values().stream().filter("COMPLETED"::equals).count();
        long failedCount = statuses.values().stream().filter("FAILED"::equals).count();

        if (completedCount + failedCount == def.getNodes().size()) {
            String finalStatus = failedCount > 0 ? "FAILED" : "COMPLETED";
            statusService.setOverallStatus(executionId, pidStr, finalStatus);
            finishExecution(executionId, pidStr, finalStatus, def, statuses);
            return;
        }

        // Trigger downstream nodes whose all dependencies are now completed
        Map<String, Set<String>> incomingEdges = buildIncomingEdges(def);
        Map<String, List<String>> outgoingEdges = buildOutgoingEdges(def);

        for (PipelineDefinition.NodeDefinition node : def.getNodes()) {
            String nid = node.getId();
            String currentStatus = statuses.getOrDefault(nid, "WAITING");
            if (!"WAITING".equals(currentStatus)) continue;

            Set<String> deps = incomingEdges.getOrDefault(nid, Set.of());
            boolean allDepsCompleted = deps.stream().allMatch(dep -> "COMPLETED".equals(statuses.get(dep)));

            if (allDepsCompleted && !deps.isEmpty()) {
                log.info("Triggering downstream node: {}", nid);
                statuses.put(nid, "QUEUED");
                TaskMessage task = buildTaskMessage(executionId, pidStr, node, def, incomingEdges, outgoingEdges);
                taskPublisher.publishTask(task);
            }
        }
    }

    private void finishExecution(String executionId, String pipelineId, String status,
                                   PipelineDefinition def, Map<String, String> statuses) {
        long startTime = executionStartTimes.getOrDefault(executionId, System.currentTimeMillis());
        long durationMs = System.currentTimeMillis() - startTime;

        Map<String, Object> nodeResults = new HashMap<>();
        for (PipelineDefinition.NodeDefinition node : def.getNodes()) {
            nodeResults.put(node.getId(), Map.of("status", statuses.getOrDefault(node.getId(), "UNKNOWN")));
        }

        auditService.recordExecutionCompleted(executionId, pipelineId, status, durationMs, 0, nodeResults);
        activeExecutions.remove(executionId);
        executionStartTimes.remove(executionId);
        log.info("Execution {} finished with status {} in {}ms", executionId, status, durationMs);
    }

    private TaskMessage buildTaskMessage(String executionId, String pipelineId,
                                          PipelineDefinition.NodeDefinition node,
                                          PipelineDefinition def,
                                          Map<String, Set<String>> incomingEdges,
                                          Map<String, List<String>> outgoingEdges) {
        TaskMessage task = new TaskMessage();
        task.setExecutionId(executionId);
        task.setPipelineId(pipelineId);
        task.setNodeId(node.getId());
        task.setNodeType(node.getEffectiveType());
        task.setConfig(node.getEffectiveConfig());

        // Input topics: edges coming INTO this node
        List<String> inputTopics = def.getEdges().stream()
                .filter(e -> e.getTarget().equals(node.getId()))
                .map(e -> KafkaConfig.edgeTopic(executionId, e.getSource(), e.getTarget()))
                .collect(Collectors.toList());
        task.setInputTopics(inputTopics);

        // Output topics: edges going OUT of this node
        List<String> outputTopics = def.getEdges().stream()
                .filter(e -> e.getSource().equals(node.getId()))
                .map(e -> KafkaConfig.edgeTopic(executionId, e.getSource(), e.getTarget()))
                .collect(Collectors.toList());
        task.setOutputTopics(outputTopics);

        return task;
    }

    private void createKafkaTopics(String executionId, PipelineDefinition def) {
        try {
            Properties props = new Properties();
            props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            try (AdminClient admin = AdminClient.create(props)) {
                List<NewTopic> topics = def.getEdges().stream()
                        .map(edge -> new NewTopic(
                                KafkaConfig.edgeTopic(executionId, edge.getSource(), edge.getTarget()),
                                3, (short) 1))
                        .collect(Collectors.toList());
                admin.createTopics(topics).all().get();
                log.info("Created {} Kafka topics for execution {}", topics.size(), executionId);
            }
        } catch (Exception e) {
            log.warn("Could not create Kafka topics (may already exist): {}", e.getMessage());
        }
    }

    private Map<String, Set<String>> buildIncomingEdges(PipelineDefinition def) {
        Map<String, Set<String>> incoming = new HashMap<>();
        for (PipelineDefinition.EdgeDefinition edge : def.getEdges()) {
            incoming.computeIfAbsent(edge.getTarget(), k -> new HashSet<>()).add(edge.getSource());
        }
        return incoming;
    }

    private Map<String, List<String>> buildOutgoingEdges(PipelineDefinition def) {
        Map<String, List<String>> outgoing = new HashMap<>();
        for (PipelineDefinition.EdgeDefinition edge : def.getEdges()) {
            outgoing.computeIfAbsent(edge.getSource(), k -> new ArrayList<>()).add(edge.getTarget());
        }
        return outgoing;
    }
}
