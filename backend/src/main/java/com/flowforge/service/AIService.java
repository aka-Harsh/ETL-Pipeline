package com.flowforge.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flowforge.config.RedisConfig;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Service
@RequiredArgsConstructor
@Slf4j
public class AIService {

    private final ObjectMapper objectMapper;
    private final StringRedisTemplate redisTemplate;

    @Value("${ai.api.url}")
    private String aiApiUrl;

    @Value("${ai.api.key:}")
    private String aiApiKey;

    @Value("${ai.api.model:llama3:8b}")
    private String aiModel;

    @Value("${ai.cache.ttl-seconds:3600}")
    private long cacheTtlSeconds;

    private static final String SYSTEM_PROMPT = """
            You are a strict JSON generator for FlowForge ETL pipelines. Output ONLY a valid JSON object. No explanation, no markdown, no code fences.

            AVAILABLE NODE TYPES:
            S3_SOURCE    config: { "bucket": string, "key": string, "format": "csv"|"json", "hasHeader": true|false }
            KAFKA_SOURCE config: { "topic": string, "consumerGroup": string }
            FILTER       config: { "field": string, "operator": string, "value": string }
            FIELD_MAPPER config: { "mappings": [{ "from": string, "to": string }] }
            AGGREGATOR   config: { "groupBy": string, "function": "count"|"sum"|"avg", "field": string }
            POSTGRES_SINK config: { "table": string, "writeMode": "append"|"upsert" }
            S3_SINK      config: { "bucket": string, "keyPrefix": string, "format": "csv"|"json" }
            KAFKA_SINK   config: { "topic": string }

            OPERATOR MAPPING — use these exact strings for FILTER operator field:
            "greater than" | "more than" | "above" | "over" | ">" → use ">"
            "less than" | "below" | "under" | "<"                 → use "<"
            "at least" | ">="                                      → use ">="
            "at most" | "<="                                       → use "<="
            "equals" | "equal to" | "is" | "=="                   → use "=="
            "not equal" | "!="                                     → use "!="
            "contains" | "includes" | "has"                       → use "contains"

            FILE KEY RULE: Always use the COMPLETE file path the user mentions, including directory prefixes.
            Example: "file input/sales.csv" → key must be "input/sales.csv" NOT "sales.csv".

            MINIMALISM RULE: Only add nodes that the user EXPLICITLY mentions. Do NOT add FIELD_MAPPER, FILTER, or any transform unless the user asks for it.

            SELECTION RULES:
            "read/load from S3" / "S3 file" / "CSV/JSON from bucket" → S3_SOURCE
            "consume/read from Kafka topic" / "stream from"           → KAFKA_SOURCE
            "filter/where/only rows where"                            → FILTER
            "rename/map columns/fields"                               → FIELD_MAPPER
            "aggregate/count by/group by/sum/average"                 → AGGREGATOR
            "save/write/store to Postgres/table/DB"                   → POSTGRES_SINK
            "save/write/output to S3/bucket"                         → S3_SINK
            "publish/send/write to Kafka topic"                       → KAFKA_SINK

            LABEL RULE: Use the label the user explicitly says (e.g. "label it Raw Input"). Otherwise make a short 2-3 word label describing the node's purpose.

            POSITIONING: x = 100 + (nodeIndex * 280), y = 200 for all nodes.

            EXAMPLE 1 — "Consume from Kafka topic orders, filter where amount is greater than 500, save to postgres table big_orders":
            {"nodes":[{"id":"node_1","type":"KAFKA_SOURCE","label":"Orders Stream","position":{"x":100,"y":200},"config":{"topic":"orders","consumerGroup":"flowforge-consumer"}},{"id":"node_2","type":"FILTER","label":"High Value Filter","position":{"x":380,"y":200},"config":{"field":"amount","operator":">","value":"500"}},{"id":"node_3","type":"POSTGRES_SINK","label":"Big Orders DB","position":{"x":660,"y":200},"config":{"table":"big_orders","writeMode":"append"}}],"edges":[{"id":"edge_1","source":"node_1","target":"node_2"},{"id":"edge_2","source":"node_2","target":"node_3"}]}

            EXAMPLE 2 — "Read sales CSV from S3 bucket mydata at key input/sales.csv, aggregate by region counting items, save results to S3 bucket myoutput prefix region-counts as json":
            {"nodes":[{"id":"node_1","type":"S3_SOURCE","label":"Sales CSV","position":{"x":100,"y":200},"config":{"bucket":"mydata","key":"input/sales.csv","format":"csv","hasHeader":true}},{"id":"node_2","type":"AGGREGATOR","label":"Count by Region","position":{"x":380,"y":200},"config":{"groupBy":"region","function":"count","field":"region"}},{"id":"node_3","type":"S3_SINK","label":"Region Counts S3","position":{"x":660,"y":200},"config":{"bucket":"myoutput","keyPrefix":"region-counts/","format":"json"}}],"edges":[{"id":"edge_1","source":"node_1","target":"node_2"},{"id":"edge_2","source":"node_2","target":"node_3"}]}

            Now generate a pipeline for the following request. Output ONLY the JSON object:
            """;

    // ── Public API ────────────────────────────────────────────────────────────────

    /**
     * Interprets a user prompt using the keyword parser and returns a human-readable
     * summary of what nodes would be created. Does NOT call the LLM — instant response.
     */
    @SuppressWarnings("unchecked")
    public Map<String, Object> interpretPrompt(String userPrompt) {
        Map<String, Object> pipeline = buildFromKeywords(userPrompt);
        if (pipeline == null) {
            return Map.of(
                "understood", false,
                "summary", "I couldn't identify a clear pipeline from your description. " +
                           "Please mention where to read data from (e.g. S3 bucket, Kafka topic) " +
                           "and where to write results (e.g. Postgres table, S3, Kafka topic)."
            );
        }
        List<Map<String, Object>> nodes = (List<Map<String, Object>>) pipeline.get("nodes");
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < nodes.size(); i++) {
            Map<String, Object> node = nodes.get(i);
            String type = (String) node.get("type");
            Map<String, Object> config = (Map<String, Object>) node.get("config");
            if (i > 0) sb.append(" → ");
            sb.append(describeNode(type, config));
        }
        return Map.of(
            "understood", true,
            "summary", sb.toString(),
            "nodeCount", nodes.size(),
            "pipeline", pipeline   // include the pre-built pipeline so frontend can use it directly
        );
    }

    private String describeNode(String type, Map<String, Object> config) {
        return switch (type) {
            case "S3_SOURCE"     -> String.format("Read %s from S3 bucket \"%s\" (key: %s)",
                                        config.getOrDefault("format", "file"),
                                        config.getOrDefault("bucket", "?"),
                                        config.getOrDefault("key", "?"));
            case "KAFKA_SOURCE"  -> String.format("Consume from Kafka topic \"%s\"",
                                        config.getOrDefault("topic", "?"));
            case "FILTER"        -> String.format("Filter where %s %s %s",
                                        config.getOrDefault("field", "?"),
                                        config.getOrDefault("operator", "?"),
                                        config.getOrDefault("value", "?"));
            case "FIELD_MAPPER"  -> {
                @SuppressWarnings("unchecked")
                List<Map<String, Object>> mappings =
                    (List<Map<String, Object>>) config.getOrDefault("mappings", List.of());
                String pairs = mappings.stream()
                    .map(m -> m.getOrDefault("from", "?") + " → " + m.getOrDefault("to", "?"))
                    .reduce((a, b) -> a + ", " + b).orElse("(no mappings)");
                yield "Rename fields: " + pairs;
            }
            case "AGGREGATOR"    -> String.format("%s by %s on field %s",
                                        config.getOrDefault("function", "count"),
                                        config.getOrDefault("groupBy", "?"),
                                        config.getOrDefault("field", "?"));
            case "POSTGRES_SINK" -> String.format("Write to Postgres table \"%s\"",
                                        config.getOrDefault("table", "?"));
            case "S3_SINK"       -> String.format("Write to S3 bucket \"%s\" (prefix: %s)",
                                        config.getOrDefault("bucket", "?"),
                                        config.getOrDefault("keyPrefix", "?"));
            case "KAFKA_SINK"    -> String.format("Publish to Kafka topic \"%s\"",
                                        config.getOrDefault("topic", "?"));
            default              -> type;
        };
    }

    /**
     * Generates a pipeline definition from a natural language description.
     * Three-level fallback: LLM → keyword parser → static demo.
     */
    public Map<String, Object> generatePipeline(String userPrompt) throws Exception {
        String promptHash = sha256(userPrompt);
        String cacheKey = RedisConfig.aiCacheKey(promptHash);

        // Check Redis cache — only serves entries that passed validation
        String cached = redisTemplate.opsForValue().get(cacheKey);
        if (cached != null) {
            log.info("AI cache hit for prompt hash: {}", promptHash);
            return objectMapper.readerForMapOf(Object.class).readValue(cached);
        }

        // Always build keyword pipeline — used both as fallback AND to patch LLM output
        Map<String, Object> keywordResult = buildFromKeywords(userPrompt);

        // Level 1: try the LLM
        Map<String, Object> result = tryLlm(userPrompt);

        // Patch: LLM often drops the last node (sink). If keyword parser found more nodes,
        // append the missing trailing nodes from keyword parser onto the LLM result.
        if (result != null && keywordResult != null) {
            result = patchMissingNodes(result, keywordResult);
        }

        // Level 2: keyword parser if LLM produced nothing usable
        if (result == null) {
            log.info("LLM unusable, using keyword parser for: {}", userPrompt);
            result = keywordResult;
        }

        // Level 3: static demo as last resort
        if (result == null) {
            log.info("Keyword parser could not determine pipeline, using demo");
            result = buildDemoPipeline();
        }

        // Cache only valid (≥2 node) results
        if (isValidPipeline(result)) {
            String serialised = objectMapper.writeValueAsString(result);
            redisTemplate.opsForValue().set(cacheKey, serialised, cacheTtlSeconds, TimeUnit.SECONDS);
        }
        return result;
    }

    // ── Level 1: LLM call ─────────────────────────────────────────────────────────

    private Map<String, Object> tryLlm(String userPrompt) {
        try {
            String raw = callOllamaApi(SYSTEM_PROMPT + "\n\nUser request: " + userPrompt);
            String json = extractJsonObject(raw);
            @SuppressWarnings("unchecked")
            Map<String, Object> def = objectMapper.readerForMapOf(Object.class).readValue(json);
            def = repairEdges(def);
            if (isValidPipeline(def)) {
                log.info("LLM produced valid pipeline with {} nodes",
                        ((List<?>) def.get("nodes")).size());
                return def;
            }
            log.warn("LLM output invalid after repair (nodes={}), falling back",
                    def.containsKey("nodes") ? ((List<?>) def.get("nodes")).size() : 0);
        } catch (Exception e) {
            log.warn("LLM call/parse failed ({}): {}", e.getClass().getSimpleName(), e.getMessage());
        }
        return null;
    }

    /** Finds the outermost JSON object in arbitrary LLM text. */
    private String extractJsonObject(String text) {
        String t = stripMarkdownFences(text);
        int start = t.indexOf('{');
        if (start == -1) return t;
        int depth = 0;
        for (int i = start; i < t.length(); i++) {
            char c = t.charAt(i);
            if (c == '{') depth++;
            else if (c == '}' && --depth == 0) return t.substring(start, i + 1);
        }
        return t.substring(start); // unclosed — let Jackson report the real error
    }

    /** Removes ```json … ``` wrappers that models sometimes add. */
    private String stripMarkdownFences(String text) {
        String t = text.trim();
        if (t.startsWith("```")) {
            int nl = t.indexOf('\n');
            if (nl != -1) t = t.substring(nl + 1);
            if (t.endsWith("```")) t = t.substring(0, t.lastIndexOf("```")).trim();
        }
        return t;
    }

    /**
     * Ensures every consecutive node pair has an edge. If the LLM generated nodes
     * but forgot edges (or generated fewer edges than needed), rebuild as a linear chain.
     */
    @SuppressWarnings("unchecked")
    private Map<String, Object> repairEdges(Map<String, Object> def) {
        List<Map<String, Object>> nodes = (List<Map<String, Object>>) def.get("nodes");
        if (nodes == null || nodes.size() < 2) return def;

        List<?> edges = (List<?>) def.getOrDefault("edges", Collections.emptyList());
        if (edges.size() >= nodes.size() - 1) return def; // edges look complete

        log.info("Repairing edges: {} nodes but only {} edges", nodes.size(), edges.size());
        List<Map<String, Object>> rebuilt = new ArrayList<>();
        for (int i = 0; i < nodes.size() - 1; i++) {
            rebuilt.add(Map.of(
                    "id", "edge_" + (i + 1),
                    "source", nodes.get(i).get("id"),
                    "target", nodes.get(i + 1).get("id")));
        }
        Map<String, Object> fixed = new HashMap<>(def);
        fixed.put("edges", rebuilt);
        return fixed;
    }

    private boolean isValidPipeline(Map<String, Object> def) {
        Object nodes = def.get("nodes");
        return nodes instanceof List && ((List<?>) nodes).size() >= 2;
    }

    /**
     * If the LLM dropped the sink (common failure), append missing trailing nodes
     * from the keyword parser. Only appends SINK-type nodes that are absent.
     */
    @SuppressWarnings("unchecked")
    private Map<String, Object> patchMissingNodes(Map<String, Object> llmResult,
                                                   Map<String, Object> kwResult) {
        List<Map<String, Object>> llmNodes = (List<Map<String, Object>>) llmResult.get("nodes");
        List<Map<String, Object>> kwNodes  = (List<Map<String, Object>>) kwResult.get("nodes");
        if (llmNodes == null || kwNodes == null) return llmResult;

        // Check if LLM is missing a sink (last kwNode is a SINK type but LLM has none)
        Set<String> sinkTypes = Set.of("POSTGRES_SINK", "S3_SINK", "KAFKA_SINK");
        Map<String, Object> kwLastNode = kwNodes.get(kwNodes.size() - 1);
        String kwLastType = (String) kwLastNode.get("type");

        boolean llmHasSink = llmNodes.stream()
                .anyMatch(n -> sinkTypes.contains(n.get("type")));

        if (!llmHasSink && sinkTypes.contains(kwLastType)) {
            log.info("Patching LLM output: appending missing {} from keyword parser", kwLastType);
            List<Map<String, Object>> patched = new ArrayList<>(llmNodes);

            // Reindex the keyword node id and position
            int idx = patched.size();
            Map<String, Object> sinkNode = new HashMap<>(kwLastNode);
            sinkNode.put("id", "node_" + (idx + 1));
            sinkNode.put("position", Map.of("x", 100 + idx * 280, "y", 200));
            patched.add(sinkNode);

            // Rebuild edges as linear chain
            List<Map<String, Object>> edges = new ArrayList<>();
            for (int i = 0; i < patched.size() - 1; i++) {
                edges.add(Map.of("id", "edge_" + (i + 1),
                                 "source", patched.get(i).get("id"),
                                 "target", patched.get(i + 1).get("id")));
            }
            Map<String, Object> fixed = new HashMap<>(llmResult);
            fixed.put("nodes", patched);
            fixed.put("edges", edges);
            return fixed;
        }
        return llmResult;
    }

    // ── Level 2: keyword-based parser ─────────────────────────────────────────────

    /**
     * Deterministic pipeline builder from natural language.
     * Returns null only if both source AND sink are unidentifiable.
     */
    Map<String, Object> buildFromKeywords(String userPrompt) {
        String p = userPrompt.toLowerCase();
        List<Map<String, Object>> nodes = new ArrayList<>();

        // ── Source ──────────────────────────────────────────────────────────────
        boolean kafkaSource = anyOf(p, "consume", "read from kafka", "stream from", "kafka topic");
        boolean s3Source    = anyOf(p, "s3 bucket", "from s3", "read from s3", "load from s3",
                                       ".csv", ".json", "bucket");

        if (kafkaSource && !s3Source) {
            String topic = wordAfter(p, "topic", "events");
            nodes.add(node(nodes.size(), "KAFKA_SOURCE", "Kafka Source",
                    map("topic", topic, "consumerGroup", "flowforge-consumer")));
        } else if (s3Source) {
            String bucket = extractBucket(p);
            String key    = extractKey(p);
            String fmt    = p.contains(".json") ? "json" : "csv";
            nodes.add(node(nodes.size(), "S3_SOURCE", "S3 Source",
                    map("bucket", bucket, "key", key, "format", fmt, "hasHeader", fmt.equals("csv"))));
        } else {
            return null; // cannot determine source
        }

        // ── Transforms (preserve order they appear in the prompt) ────────────────
        record Transform(int pos, String type) {}
        List<Transform> transforms = new ArrayList<>();

        int filterPos = firstIndex(p, "filter", "where ", "only rows");
        int mapperPos = firstIndex(p, "map the", "map ", "rename ", "field map",
                                      "fieldmapper", "field_mapper", "field mapper");
        int aggPos    = firstIndex(p, "aggregat", "count by", "group by", "sum of",
                                      "average", " avg ", " sum ");

        if (filterPos >= 0) transforms.add(new Transform(filterPos, "FILTER"));
        if (mapperPos >= 0) transforms.add(new Transform(mapperPos, "FIELD_MAPPER"));
        if (aggPos    >= 0) transforms.add(new Transform(aggPos,    "AGGREGATOR"));
        transforms.sort(Comparator.comparingInt(Transform::pos));

        for (Transform t : transforms) {
            switch (t.type()) {
                case "FILTER"      -> nodes.add(node(nodes.size(), "FILTER",      "Filter",       filterConfig(p)));
                case "FIELD_MAPPER"-> nodes.add(node(nodes.size(), "FIELD_MAPPER","Field Mapper",  mapperConfig(p)));
                case "AGGREGATOR"  -> nodes.add(node(nodes.size(), "AGGREGATOR",  "Aggregator",    aggConfig(p)));
            }
        }

        // ── Sink ────────────────────────────────────────────────────────────────
        boolean pgSink    = anyOf(p, "postgres", "postgresql", "sql format", "sql forat",
                                     " table ", "write to table", "save to table",
                                     "store to table", "save in postgres", "write to postgres",
                                     "store in postgres", "insert into");
        // S3 sink: match "save/write/output ... s3" with words in between
        boolean s3Sink    = anyOf(p, "save to s3", "write to s3", "output to s3",
                                     "write to bucket", "save to bucket", "save to the s3",
                                     "results to s3", "result to s3", "output to bucket");
        if (!s3Sink) s3Sink = p.matches(".*(?:save|write|output|store).*\\bs3\\b.*");
        // Kafka sink
        boolean kafkaSink = anyOf(p, "write to kafka", "publish to", "send to kafka",
                                     "kafka sink", "kafka output", "write to kafka topic");
        if (!kafkaSink) kafkaSink = p.matches(".*(?:write|send|publish|output).*topic.*");

        if (pgSink) {
            nodes.add(node(nodes.size(), "POSTGRES_SINK", "Postgres Sink",
                    map("table", extractTable(p), "writeMode", "append")));
        } else if (s3Sink) {
            nodes.add(node(nodes.size(), "S3_SINK", "S3 Sink",
                    map("bucket", "flowforge-output", "keyPrefix", "output/", "format", "json")));
        } else if (kafkaSink) {
            nodes.add(node(nodes.size(), "KAFKA_SINK", "Kafka Sink",
                    map("topic", extractSinkTopic(p))));
        } else {
            return null; // cannot determine sink
        }

        // ── Linear edges ────────────────────────────────────────────────────────
        List<Map<String, Object>> edges = new ArrayList<>();
        for (int i = 0; i < nodes.size() - 1; i++) {
            edges.add(Map.of("id", "edge_" + (i + 1),
                             "source", nodes.get(i).get("id"),
                             "target", nodes.get(i + 1).get("id")));
        }

        log.info("Keyword parser built {}-node pipeline", nodes.size());
        return Map.of("nodes", nodes, "edges", edges);
    }

    // ── Config extractors ─────────────────────────────────────────────────────────

    private Map<String, Object> filterConfig(String p) {
        String field = "amount", op = ">", value = "0";
        Matcher m;
        if (anyOf(p, " > ", "greater than", "more than", "above", "over")) {
            op = ">";
            m = Pattern.compile("(\\w+)\\s+(?:>|greater than|more than|above|over)\\s+(\\d+[\\d.]*)").matcher(p);
            if (m.find()) { field = m.group(1); value = m.group(2); }
        } else if (anyOf(p, " < ", "less than", "below", "under")) {
            op = "<";
            m = Pattern.compile("(\\w+)\\s+(?:<|less than|below|under)\\s+(\\d+[\\d.]*)").matcher(p);
            if (m.find()) { field = m.group(1); value = m.group(2); }
        } else if (anyOf(p, ">=", "at least")) {
            op = ">=";
            m = Pattern.compile("(\\w+)\\s+(?:>=|at least)\\s+(\\d+[\\d.]*)").matcher(p);
            if (m.find()) { field = m.group(1); value = m.group(2); }
        } else if (anyOf(p, "<=", "at most")) {
            op = "<=";
            m = Pattern.compile("(\\w+)\\s+(?:<=|at most)\\s+(\\d+[\\d.]*)").matcher(p);
            if (m.find()) { field = m.group(1); value = m.group(2); }
        } else if (anyOf(p, "equals", " == ", " is ")) {
            op = "==";
            m = Pattern.compile("(\\w+)\\s+(?:==|equals|is)\\s+(\\w+)").matcher(p);
            if (m.find()) { field = m.group(1); value = m.group(2); }
        } else if (anyOf(p, "contains", "includes")) {
            op = "contains";
            m = Pattern.compile("(\\w+)\\s+(?:contains|includes)\\s+(\\w+)").matcher(p);
            if (m.find()) { field = m.group(1); value = m.group(2); }
        }
        // Also try "where FIELD OP VALUE" pattern
        m = Pattern.compile("where\\s+(\\w+)\\s+(?:is\\s+)?(?:greater than|more than|above|over)\\s+(\\d+[\\d.]*)").matcher(p);
        if (m.find()) { field = m.group(1); value = m.group(2); op = ">"; }
        m = Pattern.compile("where\\s+(\\w+)\\s+(?:is\\s+)?(?:less than|below|under)\\s+(\\d+[\\d.]*)").matcher(p);
        if (m.find()) { field = m.group(1); value = m.group(2); op = "<"; }

        return map("field", field, "operator", op, "value", value);
    }

    private Map<String, Object> mapperConfig(String p) {
        String from = "name", to = "mapped_name";
        Matcher m = Pattern.compile("(?:map|rename)\\s+(?:the\\s+)?(\\w+)\\s+(?:field\\s+)?to\\s+(\\w+)").matcher(p);
        if (m.find()) { from = m.group(1); to = m.group(2); }
        return Map.of("mappings", List.of(map("from", from, "to", to)));
    }

    private Map<String, Object> aggConfig(String p) {
        String groupBy = "category", function = "count", field = "amount";
        Matcher m;
        m = Pattern.compile("(?:group|count)\\s+by\\s+(\\w+)").matcher(p);
        if (m.find()) groupBy = m.group(1);
        m = Pattern.compile("aggregate\\s+by\\s+(\\w+)").matcher(p);
        if (m.find()) groupBy = m.group(1);
        if (anyOf(p, " sum ", "sum of", "summing")) {
            function = "sum";
            m = Pattern.compile("sum(?:\\s+of)?\\s+(\\w+)").matcher(p);
            if (m.find()) field = m.group(1);
        } else if (anyOf(p, "average", " avg ")) {
            function = "avg";
        }
        return map("groupBy", groupBy, "function", function, "field", field);
    }

    // ── String extraction helpers ─────────────────────────────────────────────────

    private String extractBucket(String p) {
        Matcher m = Pattern.compile("bucket\\s+([\\w-]+)").matcher(p);
        if (m.find()) return m.group(1);
        return "flowforge-data";
    }

    private String extractKey(String p) {
        Matcher m = Pattern.compile("(?:at\\s+key|key)\\s+([\\w./\\-]+)").matcher(p);
        if (m.find()) return m.group(1);
        m = Pattern.compile("([\\w./\\-]+\\.(?:csv|json))").matcher(p);
        if (m.find()) return m.group(1);
        return "input/data.csv";
    }

    private String extractTable(String p) {
        // "table out_1" or "table named out_1"
        Matcher m = Pattern.compile("table\\s+(?:named\\s+)?(\\w+)").matcher(p);
        if (m.find()) return m.group(1);
        // "with table out_1"
        m = Pattern.compile("with\\s+table\\s+(\\w+)").matcher(p);
        if (m.find()) return m.group(1);
        // "postgres ... out_1" where the word after postgres/sql is the table name
        m = Pattern.compile("(?:postgres(?:ql)?|sql)\\s+(?:format\\s+)?(?:with\\s+table\\s+)?(\\w+)").matcher(p);
        if (m.find()) {
            String candidate = m.group(1);
            // skip generic words that aren't table names
            if (!candidate.matches("format|forat|table|with|named|sql|and|or|the|in|to")) return candidate;
        }
        return "results";
    }

    private String extractSinkTopic(String p) {
        // Return the last "topic X" mention (source is the first, sink is the last)
        String topic = "output-topic";
        Matcher m = Pattern.compile("topic\\s+([\\w-]+)").matcher(p);
        while (m.find()) topic = m.group(1);
        return topic;
    }

    private String wordAfter(String text, String keyword, String def) {
        Matcher m = Pattern.compile(Pattern.quote(keyword) + "\\s+([\\w-]+)").matcher(text);
        return m.find() ? m.group(1) : def;
    }

    private boolean anyOf(String text, String... keywords) {
        for (String k : keywords) if (text.contains(k)) return true;
        return false;
    }

    private int firstIndex(String text, String... keywords) {
        int min = -1;
        for (String k : keywords) {
            int i = text.indexOf(k);
            if (i >= 0 && (min == -1 || i < min)) min = i;
        }
        return min;
    }

    private Map<String, Object> node(int index, String type, String label, Map<String, Object> config) {
        return Map.of(
                "id",       "node_" + (index + 1),
                "type",     type,
                "label",    label,
                "position", Map.of("x", 100 + index * 280, "y", 200),
                "config",   config);
    }

    /** Convenience builder for small maps with mixed value types. */
    @SuppressWarnings("unchecked")
    private <K, V> Map<K, V> map(Object... kvPairs) {
        Map<Object, Object> m = new LinkedHashMap<>();
        for (int i = 0; i < kvPairs.length - 1; i += 2) m.put(kvPairs[i], kvPairs[i + 1]);
        return (Map<K, V>) m;
    }

    // ── Level 3: static demo ──────────────────────────────────────────────────────

    private Map<String, Object> buildDemoPipeline() {
        String json = """
                {"nodes":[
                  {"id":"node_1","type":"S3_SOURCE","label":"S3 Input","position":{"x":100,"y":200},
                   "config":{"bucket":"flowforge-data","key":"input/sales.csv","format":"csv","hasHeader":true}},
                  {"id":"node_2","type":"FILTER","label":"Filter","position":{"x":380,"y":200},
                   "config":{"field":"amount","operator":">","value":"500"}},
                  {"id":"node_3","type":"POSTGRES_SINK","label":"Postgres Output","position":{"x":660,"y":200},
                   "config":{"table":"results","writeMode":"append"}}],
                 "edges":[
                  {"id":"edge_1","source":"node_1","target":"node_2"},
                  {"id":"edge_2","source":"node_2","target":"node_3"}]}
                """;
        try {
            return objectMapper.readerForMapOf(Object.class).readValue(json);
        } catch (Exception e) {
            throw new RuntimeException("Failed to build demo pipeline", e);
        }
    }

    // ── LLM HTTP call ─────────────────────────────────────────────────────────────

    private String callOllamaApi(String prompt) throws Exception {
        HttpClient client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .build();

        Map<String, Object> requestBody = Map.of(
                "model",  aiModel,
                "prompt", prompt,
                "stream", false,
                "format", "json"
        );

        HttpRequest.Builder reqBuilder = HttpRequest.newBuilder()
                .uri(URI.create(aiApiUrl))
                .timeout(Duration.ofSeconds(120))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(
                        objectMapper.writeValueAsString(requestBody)));

        if (aiApiKey != null && !aiApiKey.isBlank()) {
            reqBuilder.header("Authorization", "Bearer " + aiApiKey);
        }

        HttpResponse<String> response = client.send(reqBuilder.build(),
                HttpResponse.BodyHandlers.ofString());

        if (response.statusCode() != 200) {
            throw new RuntimeException("AI API returned status " + response.statusCode());
        }

        JsonNode respNode = objectMapper.readTree(response.body());
        if (respNode.has("response")) return respNode.get("response").asText();
        if (respNode.has("choices"))
            return respNode.get("choices").get(0).get("message").get("content").asText();
        return response.body();
    }

    // ── Utilities ─────────────────────────────────────────────────────────────────

    private String sha256(String input) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        byte[] hash = digest.digest(input.getBytes(StandardCharsets.UTF_8));
        return HexFormat.of().formatHex(hash).substring(0, 16);
    }
}
