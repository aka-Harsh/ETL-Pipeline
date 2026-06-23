package com.flowforge.worker;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flowforge.config.RabbitMQConfig;
import com.flowforge.model.TaskMessage;
import com.flowforge.rabbitmq.TaskPublisher;
import com.flowforge.service.StatusService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;

@Component
@Slf4j
public class PostgresSinkExecutor extends BaseWorker {

    private final DataSource dataSource;

    public PostgresSinkExecutor(TaskPublisher taskPublisher, StatusService statusService,
                                 ObjectMapper objectMapper, DataSource dataSource) {
        super(taskPublisher, statusService, objectMapper);
        this.dataSource = dataSource;
    }

    @RabbitListener(queues = RabbitMQConfig.QUEUE_PG_SINK)
    public void handleTask(TaskMessage task) {
        log.info("PostgresSinkExecutor: executionId={} nodeId={}", task.getExecutionId(), task.getNodeId());
        statusService.setNodeStatus(task.getExecutionId(), task.getNodeId(), "RUNNING");
        long start = System.currentTimeMillis();
        try {
            long records = execute(task);
            long duration = System.currentTimeMillis() - start;
            statusService.setNodeStatus(task.getExecutionId(), task.getNodeId(), "COMPLETED", records,
                    duration > 0 ? records * 1000.0 / duration : records);
            publishSuccess(task, records, duration);
        } catch (Exception e) {
            log.error("PostgresSinkExecutor failed: {}", e.getMessage(), e);
            statusService.setNodeError(task.getExecutionId(), task.getNodeId(), e.getMessage());
            publishFailure(task, e.getMessage());
        }
    }

    @Override
    public long execute(TaskMessage task) throws Exception {
        Map<String, Object> config = task.getConfig();
        String table = (String) config.getOrDefault("table", "flowforge_output");
        // Sanitize table name
        table = table.replaceAll("[^a-zA-Z0-9_]", "_");

        String groupId = "flowforge-pgsink-" + task.getExecutionId() + "-" + task.getNodeId();
        List<String> inputRecords = drainTopics(task.getInputTopics(), groupId);

        if (inputRecords.isEmpty()) {
            log.info("No records to write to Postgres table {}", table);
            return 0;
        }

        // Determine columns from first record
        JsonNode sample = objectMapper.readTree(inputRecords.get(0));
        List<String> columns = new ArrayList<>();
        sample.fieldNames().forEachRemaining(columns::add);

        try (Connection conn = dataSource.getConnection()) {
            // Create table if not exists (using TEXT for all columns — PoC simplicity)
            createTableIfNotExists(conn, table, columns);

            // Batch insert
            String insertSql = buildInsertSql(table, columns);
            try (PreparedStatement ps = conn.prepareStatement(insertSql)) {
                for (String recordJson : inputRecords) {
                    try {
                        JsonNode node = objectMapper.readTree(recordJson);
                        for (int i = 0; i < columns.size(); i++) {
                            JsonNode val = node.get(columns.get(i));
                            ps.setString(i + 1, val != null ? val.asText() : null);
                        }
                        ps.addBatch();
                    } catch (Exception e) {
                        log.warn("Skipping malformed record: {}", e.getMessage());
                    }
                }
                ps.executeBatch();
            }
        }

        log.info("Wrote {} records to Postgres table {}", inputRecords.size(), table);
        return inputRecords.size();
    }

    private void createTableIfNotExists(Connection conn, String table, List<String> columns) throws SQLException {
        StringBuilder sb = new StringBuilder("CREATE TABLE IF NOT EXISTS ").append(table).append(" (");
        sb.append("_row_id SERIAL PRIMARY KEY, ");
        for (String col : columns) {
            String safeCol = col.replaceAll("[^a-zA-Z0-9_]", "_");
            sb.append('"').append(safeCol).append('"').append(" TEXT, ");
        }
        sb.append("inserted_at TIMESTAMP DEFAULT NOW())");
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sb.toString());
        }
    }

    private String buildInsertSql(String table, List<String> columns) {
        StringBuilder sb = new StringBuilder("INSERT INTO ").append(table).append(" (");
        sb.append(String.join(", ", columns.stream()
                .map(c -> "\"" + c.replaceAll("[^a-zA-Z0-9_]", "_") + "\"")
                .toList()));
        sb.append(") VALUES (");
        sb.append("?,".repeat(columns.size()));
        sb.deleteCharAt(sb.length() - 1);
        sb.append(")");
        return sb.toString();
    }
}
