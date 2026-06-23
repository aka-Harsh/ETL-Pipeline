package com.flowforge.flink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Flink job that reads records from Kafka, groups by a field, and applies an
 * aggregation function (count, sum, avg), writing results to an output topic.
 *
 * Arguments:
 *   --input-topic    Source Kafka topic
 *   --output-topic   Sink Kafka topic
 *   --bootstrap      Kafka bootstrap servers
 *   --group-by       Field to group by
 *   --function       Aggregation: count, sum, avg
 *   --field          Field to aggregate on (for sum/avg)
 *   --window-size    Window size in seconds (default: 10)
 *   --execution-id   Execution ID
 */
public class AggregatorJob {

    private static final Logger LOG = LoggerFactory.getLogger(AggregatorJob.class);

    public static void main(String[] args) throws Exception {
        String inputTopic = getArg(args, "--input-topic", "input");
        String outputTopic = getArg(args, "--output-topic", "output");
        String bootstrapServers = getArg(args, "--bootstrap", "localhost:9092");
        String groupByField = getArg(args, "--group-by", "category");
        String function = getArg(args, "--function", "count");
        String aggField = getArg(args, "--field", "amount");
        // window-size arg kept for backwards compatibility but not used (bounded input)
        // int windowSize = Integer.parseInt(getArg(args, "--window-size", "10"));
        String executionId = getArg(args, "--execution-id", "exec-default");

        LOG.info("Starting AggregatorJob: groupBy={} function={} field={}",
                groupByField, function, aggField);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // Kafka Source — bounded: read all records already in the topic then stop
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(inputTopic)
                .setGroupId("flowforge-aggregator-" + executionId)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setBounded(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> inputStream = env.fromSource(
                source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        // With bounded input, use keyBy+reduce (no window needed — stream terminates)
        DataStream<String> output = inputStream
                .map(new RecordToAccumulator(groupByField, aggField))
                .keyBy(t -> t.f0)
                .reduce(new AggregateReducer())
                .map(new AccumulatorToJson(function, groupByField, aggField));

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

        output.sinkTo(sink);

        env.execute("FlowForge AggregatorJob [" + executionId + "]");
    }

    /**
     * Accumulator for aggregation state per group key.
     */
    public static class AggAccumulator implements java.io.Serializable {
        public String key;
        public long count;
        public double sum;
        public double min;
        public double max;

        public AggAccumulator() {}

        public AggAccumulator(String key, double value) {
            this.key = key;
            this.count = 1;
            this.sum = value;
            this.min = value;
            this.max = value;
        }
    }

    /**
     * Maps a JSON record to an (groupKey, AggAccumulator) tuple.
     */
    public static class RecordToAccumulator
            implements MapFunction<String, Tuple2<String, AggAccumulator>> {

        private final String groupByField;
        private final String aggField;
        private transient ObjectMapper mapper;

        public RecordToAccumulator(String groupByField, String aggField) {
            this.groupByField = groupByField;
            this.aggField = aggField;
        }

        @Override
        public Tuple2<String, AggAccumulator> map(String record) throws Exception {
            if (mapper == null) mapper = new ObjectMapper();
            JsonNode node = mapper.readTree(record);
            String key = node.has(groupByField) ? node.get(groupByField).asText() : "unknown";
            double value = node.has(aggField) ? node.get(aggField).asDouble() : 0.0;
            return Tuple2.of(key, new AggAccumulator(key, value));
        }
    }

    /**
     * Reduces two accumulators into one (merges aggregation state).
     */
    public static class AggregateReducer
            implements ReduceFunction<Tuple2<String, AggAccumulator>> {

        @Override
        public Tuple2<String, AggAccumulator> reduce(
                Tuple2<String, AggAccumulator> a, Tuple2<String, AggAccumulator> b) {
            AggAccumulator merged = new AggAccumulator();
            merged.key = a.f1.key;
            merged.count = a.f1.count + b.f1.count;
            merged.sum = a.f1.sum + b.f1.sum;
            merged.min = Math.min(a.f1.min, b.f1.min);
            merged.max = Math.max(a.f1.max, b.f1.max);
            return Tuple2.of(a.f0, merged);
        }
    }

    /**
     * Converts an accumulator tuple to a JSON output string.
     */
    public static class AccumulatorToJson
            implements MapFunction<Tuple2<String, AggAccumulator>, String> {

        private final String function;
        private final String groupByField;
        private final String aggField;
        private transient ObjectMapper mapper;

        public AccumulatorToJson(String function, String groupByField, String aggField) {
            this.function = function;
            this.groupByField = groupByField;
            this.aggField = aggField;
        }

        @Override
        public String map(Tuple2<String, AggAccumulator> t) throws Exception {
            if (mapper == null) mapper = new ObjectMapper();
            ObjectNode out = mapper.createObjectNode();
            AggAccumulator acc = t.f1;
            out.put(groupByField, acc.key);
            out.put("count", acc.count);
            switch (function) {
                case "sum":
                    out.put(aggField + "_sum", acc.sum);
                    break;
                case "avg":
                    out.put(aggField + "_avg", acc.count > 0 ? acc.sum / acc.count : 0.0);
                    break;
                case "count":
                default:
                    // count already added
                    break;
            }
            return mapper.writeValueAsString(out);
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
