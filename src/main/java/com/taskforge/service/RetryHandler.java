package com.taskforge.service;

import com.taskforge.model.Task;
import com.taskforge.repository.TaskRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.UUID;

/**
 * RetryHandler - Manages task retry logic with exponential backoff.
 * 
 * Features:
 * - Exponential backoff delay calculation (1s, 2s, 4s, 8s, 16s...)
 * - Max retry limit enforcement
 * - Dead Letter Queue (DLQ) handling
 * - Retry count tracking
 * 
 * Backoff Formula: delay = baseDelay * (2 ^ retryCount)
 * Example: 1s → 2s → 4s → 8s → 16s
 */
@Service
public class RetryHandler {

    private static final Logger log = LoggerFactory.getLogger(RetryHandler.class);

    private final TaskRepository taskRepository;

    @Value("${taskforge.retry.base-delay-seconds:1}")
    private int baseDelaySeconds;

    @Value("${taskforge.retry.max-delay-seconds:300}")
    private int maxDelaySeconds;

    public RetryHandler(TaskRepository taskRepository) {
        this.taskRepository = taskRepository;
    }

    /**
     * Check if a task should be retried based on current retry count.
     * 
     * @param task Task to check
     * @return true if retry count < max retries, false otherwise
     */
    public boolean shouldRetry(Task task) {
        if (task.getRetryCount() == null) {
            task.setRetryCount(0);
        }
        
        if (task.getMaxRetries() == null) {
            task.setMaxRetries(3);
        }

        boolean shouldRetry = task.getRetryCount() < task.getMaxRetries();
        
        log.debug("Task {} retry check: count={}, max={}, shouldRetry={}", 
                task.getId(), task.getRetryCount(), task.getMaxRetries(), shouldRetry);
        
        return shouldRetry;
    }

    /**
     * Calculate exponential backoff delay for retry.
     * Formula: delay = baseDelay * (2 ^ retryCount)
     * 
     * @param retryCount Current retry attempt (0-indexed)
     * @return Delay in seconds, capped at maxDelaySeconds
     */
    public int calculateBackoffDelay(int retryCount) {
        // Exponential backoff: 1s, 2s, 4s, 8s, 16s, 32s, 64s...
        long delay = (long) (baseDelaySeconds * Math.pow(2, retryCount));
        
        // Cap at max delay
        if (delay > maxDelaySeconds) {
            delay = maxDelaySeconds;
        }
        
        log.debug("Calculated backoff delay for retry {}: {}s", retryCount, delay);
        return (int) delay;
    }

    /**
     * Calculate next retry time based on current time and backoff delay.
     * 
     * @param retryCount Current retry attempt
     * @return LocalDateTime for next retry
     */
    public LocalDateTime calculateNextRetryTime(int retryCount) {
        int delaySeconds = calculateBackoffDelay(retryCount);
        LocalDateTime nextRetry = LocalDateTime.now().plusSeconds(delaySeconds);
        
        log.debug("Next retry scheduled at {} (delay: {}s)", nextRetry, delaySeconds);
        return nextRetry;
    }

    /**
     * Prepare task for retry.
     * Updates retry count, status, scheduled time, and error message.
     * 
     * @param task Task to retry
     * @param errorMessage Error message from failed attempt
     * @return Updated task ready for retry
     */
    public Task prepareForRetry(Task task, String errorMessage) {
        if (!shouldRetry(task)) {
            log.warn("Task {} has exceeded max retries ({}). Moving to DLQ.", 
                    task.getId(), task.getMaxRetries());
            return prepareForDLQ(task, errorMessage);
        }

        // Increment retry count
        int currentRetry = task.getRetryCount() != null ? task.getRetryCount() : 0;
        task.setRetryCount(currentRetry + 1);

        // Calculate next retry time with exponential backoff
        LocalDateTime nextRetry = calculateNextRetryTime(currentRetry);
        task.setScheduledAt(nextRetry);

        // Update status and error message
        task.setStatus(Task.TaskStatus.RETRYING);
        task.setErrorMessage(String.format(
                "Retry %d/%d - Previous error: %s", 
                task.getRetryCount(), 
                task.getMaxRetries(), 
                errorMessage
        ));

        // Save to database
        task = taskRepository.save(task);

        log.info("🔄 Task {} prepared for retry {}/{} at {} (delay: {}s)", 
                task.getId(), 
                task.getRetryCount(), 
                task.getMaxRetries(),
                nextRetry,
                calculateBackoffDelay(currentRetry));

        return task;
    }

    /**
     * Prepare task for Dead Letter Queue (DLQ).
     * Called when max retries exceeded.
     * 
     * @param task Task that has failed permanently
     * @param errorMessage Final error message
     * @return Updated task marked as DEAD
     */
    public Task prepareForDLQ(Task task, String errorMessage) {
        task.setStatus(Task.TaskStatus.DEAD);
        task.setErrorMessage(String.format(
                "Max retries (%d) exceeded. Final error: %s",
                task.getMaxRetries(),
                errorMessage
        ));
        task.setCompletedAt(LocalDateTime.now());

        task = taskRepository.save(task);

        log.error("💀 Task {} moved to DLQ after {} failed attempts. Error: {}", 
                task.getId(), task.getRetryCount(), errorMessage);

        return task;
    }

    /**
     * Handle failed task execution.
     * Decides whether to retry or move to DLQ.
     * 
     * @param taskId Task UUID
     * @param errorMessage Error message from failed execution
     * @return Updated task
     */
    public Task handleFailure(UUID taskId, String errorMessage) {
        Task task = taskRepository.findById(taskId)
                .orElseThrow(() -> new RuntimeException("Task not found: " + taskId));

        if (shouldRetry(task)) {
            return prepareForRetry(task, errorMessage);
        } else {
            return prepareForDLQ(task, errorMessage);
        }
    }

    /**
     * Get retry statistics for a task.
     * 
     * @param task Task to analyze
     * @return Retry statistics summary
     */
    public RetryStats getRetryStats(Task task) {
        int currentRetry = task.getRetryCount() != null ? task.getRetryCount() : 0;
        int maxRetries = task.getMaxRetries() != null ? task.getMaxRetries() : 3;
        int remainingRetries = Math.max(0, maxRetries - currentRetry);
        
        int nextDelay = currentRetry < maxRetries 
                ? calculateBackoffDelay(currentRetry) 
                : 0;

        return new RetryStats(
                currentRetry,
                maxRetries,
                remainingRetries,
                nextDelay,
                shouldRetry(task)
        );
    }

    /**
     * Retry statistics data class.
     */
    public static class RetryStats {
        private final int currentRetry;
        private final int maxRetries;
        private final int remainingRetries;
        private final int nextDelaySeconds;
        private final boolean canRetry;

        public RetryStats(int currentRetry, int maxRetries, int remainingRetries, 
                         int nextDelaySeconds, boolean canRetry) {
            this.currentRetry = currentRetry;
            this.maxRetries = maxRetries;
            this.remainingRetries = remainingRetries;
            this.nextDelaySeconds = nextDelaySeconds;
            this.canRetry = canRetry;
        }

        public int getCurrentRetry() {
            return currentRetry;
        }

        public int getMaxRetries() {
            return maxRetries;
        }

        public int getRemainingRetries() {
            return remainingRetries;
        }

        public int getNextDelaySeconds() {
            return nextDelaySeconds;
        }

        public boolean isCanRetry() {
            return canRetry;
        }

        @Override
        public String toString() {
            return String.format(
                    "RetryStats{current=%d, max=%d, remaining=%d, nextDelay=%ds, canRetry=%s}",
                    currentRetry, maxRetries, remainingRetries, nextDelaySeconds, canRetry
            );
        }
    }
}
