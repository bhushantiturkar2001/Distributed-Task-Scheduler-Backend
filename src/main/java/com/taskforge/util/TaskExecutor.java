package com.taskforge.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.taskforge.model.Task;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.*;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * TaskExecutor - Executes different types of tasks
 * Currently supports: HTTP_CALL, LOG, CUSTOM
 */
@Component
@Slf4j
public class TaskExecutor {

    private final RestTemplate restTemplate;
    private final ObjectMapper objectMapper;

    public TaskExecutor() {
        this.restTemplate = new RestTemplate();
        this.objectMapper = new ObjectMapper();
    }

    /**
     * Execute a task based on its type
     * 
     * @param task The task to execute
     * @return ExecutionResult with success status and output
     */
    public ExecutionResult execute(Task task) {
        log.info("🚀 Executing task - ID: {}, Type: {}", task.getId(), task.getTaskType());

        try {
            return switch (task.getTaskType()) {
                case HTTP_CALL -> executeHttpCall(task);
                case LOG -> executeLog(task);
                case CUSTOM -> executeCustom(task);
            };
        } catch (Exception e) {
            log.error("❌ Task execution failed - ID: {}, Error: {}", task.getId(), e.getMessage(), e);
            return ExecutionResult.failure("Execution failed: " + e.getMessage());
        }
    }

    /**
     * Execute HTTP_CALL task type
     * Makes an HTTP request based on payload configuration
     * 
     * Payload format:
     * {
     *   "url": "https://api.example.com/endpoint",
     *   "method": "POST",
     *   "headers": {"Authorization": "Bearer token"},
     *   "body": {"key": "value"}
     * }
     * 
     * @param task The task with HTTP call configuration
     * @return ExecutionResult
     */
    private ExecutionResult executeHttpCall(Task task) {
        try {
            log.info("📡 Making HTTP call for task: {}", task.getId());

            // Parse payload JSON
            JsonNode payloadNode = objectMapper.readTree(task.getPayload());

            // Extract request details
            String url = payloadNode.get("url").asText();
            String method = payloadNode.has("method") ? payloadNode.get("method").asText() : "GET";
            
            HttpMethod httpMethod = HttpMethod.valueOf(method.toUpperCase());

            // Build headers
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            
            if (payloadNode.has("headers")) {
                JsonNode headersNode = payloadNode.get("headers");
                headersNode.fields().forEachRemaining(entry -> 
                    headers.add(entry.getKey(), entry.getValue().asText())
                );
            }

            // Build request body
            String requestBody = null;
            if (payloadNode.has("body")) {
                requestBody = objectMapper.writeValueAsString(payloadNode.get("body"));
            }

            HttpEntity<String> requestEntity = new HttpEntity<>(requestBody, headers);

            log.debug("HTTP Request - URL: {}, Method: {}", url, httpMethod);

            // Make HTTP call
            ResponseEntity<String> response = restTemplate.exchange(
                url, 
                httpMethod, 
                requestEntity, 
                String.class
            );

            log.info("✅ HTTP call successful - Status: {}, Task: {}", 
                response.getStatusCode(), task.getId());

            return ExecutionResult.success(
                String.format("HTTP %s %s - Status: %d, Response: %s", 
                    method, url, response.getStatusCode().value(), 
                    truncate(response.getBody(), 200))
            );

        } catch (HttpClientErrorException e) {
            log.error("❌ HTTP Client Error - Status: {}, Body: {}", 
                e.getStatusCode(), e.getResponseBodyAsString());
            return ExecutionResult.failure(
                String.format("HTTP %d: %s", e.getStatusCode().value(), e.getMessage())
            );
        } catch (HttpServerErrorException e) {
            log.error("❌ HTTP Server Error - Status: {}, Body: {}", 
                e.getStatusCode(), e.getResponseBodyAsString());
            return ExecutionResult.failure(
                String.format("HTTP %d: %s", e.getStatusCode().value(), e.getMessage())
            );
        } catch (ResourceAccessException e) {
            log.error("❌ HTTP Connection Error: {}", e.getMessage());
            return ExecutionResult.failure("Connection failed: " + e.getMessage());
        } catch (Exception e) {
            log.error("❌ HTTP call failed: {}", e.getMessage(), e);
            return ExecutionResult.failure("HTTP call failed: " + e.getMessage());
        }
    }

    /**
     * Execute LOG task type
     * Simply logs the payload content
     * 
     * @param task The task with log content
     * @return ExecutionResult
     */
    private ExecutionResult executeLog(Task task) {
        try {
            log.info("📝 LOG Task - ID: {}", task.getId());
            log.info("📝 Payload: {}", task.getPayload());
            
            return ExecutionResult.success("Logged: " + truncate(task.getPayload(), 200));
        } catch (Exception e) {
            log.error("❌ Log task failed: {}", e.getMessage());
            return ExecutionResult.failure("Log failed: " + e.getMessage());
        }
    }

    /**
     * Execute CUSTOM task type
     * Placeholder for custom task execution logic
     * 
     * @param task The task with custom logic
     * @return ExecutionResult
     */
    private ExecutionResult executeCustom(Task task) {
        try {
            log.info("⚙️ CUSTOM Task - ID: {}", task.getId());
            log.info("⚙️ Payload: {}", task.getPayload());
            
            // Future: Add custom execution logic here
            // Could invoke different handlers based on payload configuration
            
            return ExecutionResult.success("Custom task executed: " + truncate(task.getPayload(), 200));
        } catch (Exception e) {
            log.error("❌ Custom task failed: {}", e.getMessage());
            return ExecutionResult.failure("Custom execution failed: " + e.getMessage());
        }
    }

    /**
     * Truncate string to max length
     */
    private String truncate(String str, int maxLength) {
        if (str == null) return "";
        return str.length() > maxLength ? str.substring(0, maxLength) + "..." : str;
    }

    /**
     * Result of task execution
     */
    public static class ExecutionResult {
        private final boolean success;
        private final String output;
        private final String errorMessage;

        private ExecutionResult(boolean success, String output, String errorMessage) {
            this.success = success;
            this.output = output;
            this.errorMessage = errorMessage;
        }

        public static ExecutionResult success(String output) {
            return new ExecutionResult(true, output, null);
        }

        public static ExecutionResult failure(String errorMessage) {
            return new ExecutionResult(false, null, errorMessage);
        }

        public boolean isSuccess() {
            return success;
        }

        public String getOutput() {
            return output;
        }

        public String getErrorMessage() {
            return errorMessage;
        }

        @Override
        public String toString() {
            return success 
                ? "SUCCESS: " + output 
                : "FAILED: " + errorMessage;
        }
    }
}
