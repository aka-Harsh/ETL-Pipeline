package com.flowforge.model;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

@Data
@NoArgsConstructor
public class PipelineDefinition {
    private List<NodeDefinition> nodes;
    private List<EdgeDefinition> edges;

    @Data
    @NoArgsConstructor
    public static class NodeDefinition {
        private String id;
        private String type;
        private String label;
        private Position position;
        private Map<String, Object> data;
        private Map<String, Object> config;

        public Map<String, Object> getEffectiveConfig() {
            // Config may be nested inside data.config (React Flow format)
            if (config != null) return config;
            if (data != null && data.containsKey("config")) {
                @SuppressWarnings("unchecked")
                Map<String, Object> nested = (Map<String, Object>) data.get("config");
                return nested;
            }
            return Map.of();
        }

        public String getEffectiveLabel() {
            if (label != null) return label;
            if (data != null && data.containsKey("label")) return (String) data.get("label");
            return type;
        }

        public String getEffectiveType() {
            if (type != null) return type;
            if (data != null && data.containsKey("nodeType")) return (String) data.get("nodeType");
            return "UNKNOWN";
        }
    }

    @Data
    @NoArgsConstructor
    public static class EdgeDefinition {
        private String id;
        private String source;
        private String target;
    }

    @Data
    @NoArgsConstructor
    public static class Position {
        private double x;
        private double y;
    }
}
