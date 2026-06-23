package com.flowforge.flink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Flink job that reads records from an input Kafka topic, applies a filter predicate,
 * and writes matching records to an output Kafka topic.
 *
 * Arguments:
 *   --input-topic    Source Kafka topic
 *   --output-topic   Sink Kafka topic
 *   --bootstrap      Kafka bootstrap servers
 *   --field          Field name to filter on
 *   --operator       Operator: ==, !=, >, <, contains
 *   --value          Value to compare against
 *   --execution-id   Execution ID (used as consumer group prefix)
 */
public class FilterJob {

    private static final Logger LOG = LoggerFactory.getLogger(FilterJob.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        // Parse args
        String inputTopic = getArg(args, "--input-topic", "input");
        String outputTopic = getArg(args, "--output-topic", "output");
        String bootstrapServers = getArg(args, "--bootstrap", "localhost:9092");
        String field = getArg(args, "--field", "amount");
        String operator = getArg(args, "--operator", ">");
        String value = getArg(args, "--value", "0");
        String executionId = getArg(args, "--execution-id", "exec-default");

        LOG.info("Starting FilterJob: {} {} {} on topic {} → {}",
                field, operator, value, inputTopic, outputTopic);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // Kafka Source — bounded: read all records already in the topic then stop
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(inputTopic)
                .setGroupId("flowforge-filter-" + executionId)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setBounded(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> inputStream = env.fromSource(
                source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        // Apply filter
        DataStream<String> filtered = inputStream.filter(new JsonFieldFilter(field, operator, value));

        // Kafka Sink
        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic(outputTopic)
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                .build();

        filtered.sinkTo(sink);

        env.execute("FlowForge FilterJob [" + executionId + "]");
    }

    /**
     * Flink FilterFunction that evaluates a field-based predicate on JSON records.
     */
    public static class JsonFieldFilter implements FilterFunction<String> {
        private final String field;
        private final String operator;
        private final String value;
        private transient ObjectMapper mapper;

        public JsonFieldFilter(String field, String operator, String value) {
            this.field = field;
            this.operator = operator;
            this.value = value;
        }

        @Override
        public boolean filter(String record) {
            if (mapper == null) {
                mapper = new ObjectMapper();
            }
            try {
                JsonNode node = mapper.readTree(record);
                JsonNode fieldNode = node.get(field);
                if (fieldNode == null) {
                    return false;
                }
                String fieldValue = fieldNode.asText();
                return evaluate(fieldValue);
            } catch (Exception e) {
                LOG.warn("Failed to parse record for filtering: {}", record, e);
                return false;
            }
        }

        private boolean evaluate(String fieldValue) {
            switch (operator) {
                case "==":
                    return fieldValue.equals(value);
                case "!=":
                    return !fieldValue.equals(value);
                case "contains":
                    return fieldValue.contains(value);
                case ">":
                    return parseDouble(fieldValue) > parseDouble(value);
                case "<":
                    return parseDouble(fieldValue) < parseDouble(value);
                case ">=":
                    return parseDouble(fieldValue) >= parseDouble(value);
                case "<=":
                    return parseDouble(fieldValue) <= parseDouble(value);
                default:
                    LOG.warn("Unknown operator: {}", operator);
                    return false;
            }
        }

        private double parseDouble(String s) {
            try {
                return Double.parseDouble(s);
            } catch (NumberFormatException e) {
                return 0.0;
            }
        }
    }

    private static String getArg(String[] args, String key, String defaultValue) {
        for (int i = 0; i < args.length - 1; i++) {
            if (args[i].equals(key)) {
                return args[i + 1];
            }
        }
        return defaultValue;
    }
}
