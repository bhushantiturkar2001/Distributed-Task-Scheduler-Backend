package com.taskforge.service;

import com.taskforge.model.ExecutionLog;
import com.taskforge.model.Task;
import com.taskforge.repository.ExecutionLogRepository;
import com.taskforge.util.TaskExecutor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.net.InetAddress;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

/**
 * ExecutionLogService - Manages execution logs for task runs
 * Tracks execution history, duration, outputs, and errors
 */
@Service
@Slf4j
public class ExecutionLogService {

    private final ExecutionLogRepository executionLogRepository;
    private final String workerId;

    public ExecutionLogService(ExecutionLogRepository executionLogRepository) {
        this.executionLogRepository = executionLogRepository;
        this.workerId = generateWorkerId();
    }

    /**
     * Create and save an execution log for a task
     * 
     * @param task The task that was executed
     * @param result The execution result
     * @param startTime When execution started
     * @param endTime When execution ended
     * @param attemptNumber Which retry attempt this was
     * @return Saved ExecutionLog
     */
    @Transactional
    public ExecutionLog logExecution(
            Task task, 
            TaskExecutor.ExecutionResult result,
            LocalDateTime startTime,
            LocalDateTime endTime,
            int attemptNumber) {

        log.debug("Creating execution log for task: {}, attempt: {}", task.getId(), attemptNumber);

        // Calculate duration
        long durationMs = Duration.between(startTime, endTime).toMillis();

        // Build execution log
        ExecutionLog executionLog = ExecutionLog.builder()
                .taskId(task.getId())
                .workerId(workerId)
                .status(result.isSuccess() ? ExecutionLog.ExecutionStatus.SUCCESS : ExecutionLog.ExecutionStatus.FAILED)
                .startedAt(startTime)
                .completedAt(endTime)
                .durationMs(durationMs)
                .output(result.isSuccess() ? truncate(result.getOutput(), 1000) : null)
                .errorMessage(result.isSuccess() ? null : truncate(result.getErrorMessage(), 1000))
                .attemptNumber(attemptNumber)
                .build();

        ExecutionLog saved = executionLogRepository.save(executionLog);
        
        log.info("📝 Execution log created - Task: {}, Attempt: {}, Status: {}, Duration: {}ms", 
                task.getId(), attemptNumber, saved.getStatus(), durationMs);

        return saved;
    }

    /**
     * Get all execution logs for a specific task
     * Ordered by creation time descending (newest first)
     * 
     * @param taskId The task ID
     * @param pageable Pagination parameters
     * @return Page of execution logs
     */
    @Transactional(readOnly = true)
    public Page<ExecutionLog> getExecutionLogsByTaskId(UUID taskId, Pageable pageable) {
        log.debug("Fetching execution logs for task: {}", taskId);
        return executionLogRepository.findByTaskIdOrderByCreatedAtDesc(taskId, pageable);
    }

    /**
     * Get all execution logs for a specific task
     * 
     * @param taskId The task ID
     * @return List of execution logs
     */
    @Transactional(readOnly = true)
    public List<ExecutionLog> getAllExecutionLogsByTaskId(UUID taskId) {
        log.debug("Fetching all execution logs for task: {}", taskId);
        return executionLogRepository.findByTaskIdOrderByAttemptNumberDesc(taskId);
    }

    /**
     * Get execution logs by worker ID
     * Useful for monitoring worker performance
     * 
     * @param workerId The worker ID
     * @return List of execution logs
     */
    @Transactional(readOnly = true)
    public List<ExecutionLog> getExecutionLogsByWorkerId(String workerId) {
        log.debug("Fetching execution logs for worker: {}", workerId);
        return executionLogRepository.findByWorkerIdOrderByCreatedAtDesc(workerId);
    }

    /**
     * Get the latest execution log for a task
     * 
     * @param taskId The task ID
     * @return Latest ExecutionLog or null if none exists
     */
    @Transactional(readOnly = true)
    public ExecutionLog getLatestExecutionLog(UUID taskId) {
        log.debug("Fetching latest execution log for task: {}", taskId);
        List<ExecutionLog> logs = executionLogRepository.findByTaskIdOrderByAttemptNumberDesc(taskId);
        return logs.isEmpty() ? null : logs.get(0);
    }

    /**
     * Count execution logs by status
     * 
     * @param status The status (SUCCESS or FAILED)
     * @return Count of logs
     */
    @Transactional(readOnly = true)
    public long countByStatus(ExecutionLog.ExecutionStatus status) {
        return executionLogRepository.countByStatus(status);
    }

    /**
     * Get execution statistics for a task
     * 
     * @param taskId The task ID
     * @return ExecutionStats object
     */
    @Transactional(readOnly = true)
    public ExecutionStats getExecutionStats(UUID taskId) {
        List<ExecutionLog> logs = executionLogRepository.findByTaskIdOrderByAttemptNumberDesc(taskId);
        
        if (logs.isEmpty()) {
            return new ExecutionStats(0, 0, 0, 0L, 0L);
        }

        long totalRuns = logs.size();
        long successCount = logs.stream().filter(log -> ExecutionLog.ExecutionStatus.SUCCESS.equals(log.getStatus())).count();
        long failedCount = logs.stream().filter(log -> ExecutionLog.ExecutionStatus.FAILED.equals(log.getStatus())).count();
        
        long totalDuration = logs.stream()
                .filter(log -> log.getDurationMs() != null)
                .mapToLong(ExecutionLog::getDurationMs)
                .sum();
        
        long avgDuration = totalRuns > 0 ? totalDuration / totalRuns : 0;

        return new ExecutionStats(totalRuns, successCount, failedCount, avgDuration, totalDuration);
    }

    /**
     * Get current worker ID
     * 
     * @return Worker ID
     */
    public String getWorkerId() {
        return workerId;
    }

    /**
     * Generate a unique worker ID based on hostname and timestamp
     * 
     * @return Worker ID string
     */
    private String generateWorkerId() {
        try {
            String hostname = InetAddress.getLocalHost().getHostName();
            String timestamp = String.valueOf(System.currentTimeMillis() % 10000);
            String workerId = hostname + "-" + timestamp;
            log.info("🤖 Worker ID: {}", workerId);
            return workerId;
        } catch (Exception e) {
            String fallbackId = "worker-" + System.currentTimeMillis() % 10000;
            log.warn("Failed to get hostname, using fallback worker ID: {}", fallbackId);
            return fallbackId;
        }
    }

    /**
     * Truncate string to max length
     */
    private String truncate(String str, int maxLength) {
        if (str == null) return null;
        return str.length() > maxLength ? str.substring(0, maxLength) + "..." : str;
    }

    /**
     * Execution statistics for a task
     */
    public static class ExecutionStats {
        private final long totalRuns;
        private final long successCount;
        private final long failedCount;
        private final long avgDurationMs;
        private final long totalDurationMs;

        public ExecutionStats(long totalRuns, long successCount, long failedCount, 
                            long avgDurationMs, long totalDurationMs) {
            this.totalRuns = totalRuns;
            this.successCount = successCount;
            this.failedCount = failedCount;
            this.avgDurationMs = avgDurationMs;
            this.totalDurationMs = totalDurationMs;
        }

        public long getTotalRuns() { return totalRuns; }
        public long getSuccessCount() { return successCount; }
        public long getFailedCount() { return failedCount; }
        public long getAvgDurationMs() { return avgDurationMs; }
        public long getTotalDurationMs() { return totalDurationMs; }
        public double getSuccessRate() { 
            return totalRuns > 0 ? (double) successCount / totalRuns * 100 : 0; 
        }

        @Override
        public String toString() {
            return String.format("ExecutionStats{runs=%d, success=%d, failed=%d, avgDuration=%dms, successRate=%.1f%%}",
                    totalRuns, successCount, failedCount, avgDurationMs, getSuccessRate());
        }
    }
}
