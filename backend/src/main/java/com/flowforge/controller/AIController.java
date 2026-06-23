package com.flowforge.controller;

import com.flowforge.service.AIService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/ai")
@RequiredArgsConstructor
@Slf4j
public class AIController {

    private final AIService aiService;

    @PostMapping("/interpret-prompt")
    public ResponseEntity<?> interpretPrompt(@RequestBody Map<String, String> request) {
        String prompt = request.get("prompt");
        if (prompt == null || prompt.isBlank()) {
            return ResponseEntity.badRequest().body(Map.of("error", "prompt is required"));
        }
        return ResponseEntity.ok(aiService.interpretPrompt(prompt));
    }

    @PostMapping("/generate-pipeline")
    public ResponseEntity<?> generatePipeline(@RequestBody Map<String, String> request) {
        String prompt = request.get("prompt");
        if (prompt == null || prompt.isBlank()) {
            return ResponseEntity.badRequest().body(Map.of("error", "prompt is required"));
        }
        log.info("AI generate-pipeline request: {}", prompt.substring(0, Math.min(80, prompt.length())));
        try {
            Map<String, Object> pipeline = aiService.generatePipeline(prompt);
            return ResponseEntity.ok(pipeline);
        } catch (Exception e) {
            log.error("AI pipeline generation failed: {}", e.getMessage());
            return ResponseEntity.internalServerError().body(Map.of("error", e.getMessage()));
        }
    }
}
