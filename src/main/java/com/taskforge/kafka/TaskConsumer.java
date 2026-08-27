package com.taskforge.kafka;

import com.taskforge.config.KafkaConfig;
import com.taskforge.kafka.TaskProducer;
import com.taskforge.model.Task;
import com.taskforge.repository.TaskRepository;
import com.taskforge.service.ExecutionLogService;
import com.taskforge.service.RedisLockManager;
import com.taskforge.service.RetryHandler;
import com.taskforge.util.TaskExecutor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

/**
 * TaskConsumer - Kafka listener that consumes tasks from the execution queue
 * Represents a worker node that processes tasks
 * 
 * Features:
 * - Distributed locking to prevent duplicate execution
 * - Concurrent processing with multiple consumer threads
 * - Automatic execution logging
 * - Retry mechanism with exponential backoff
 * - Dead Letter Queue handling
 */
@Component
@Slf4j
public class TaskConsumer {

    private final TaskRepository taskRepository;
    private final TaskExecutor taskExecutor;
    private final ExecutionLogService executionLogService;
    private final RedisLockManager lockManager;
    private final RetryHandler retryHandler;
    private final TaskProducer taskProducer;

    public TaskConsumer(TaskRepository taskRepository, 
                       TaskExecutor taskExecutor,
                       ExecutionLogService executionLogService,
                       RedisLockManager lockManager,
                       RetryHandler retryHandler,
                       TaskProducer taskProducer) {
        this.taskRepository = taskRepository;
        this.taskExecutor = taskExecutor;
        this.executionLogService = executionLogService;
        this.lockManager = lockManager;
        this.retryHandler = retryHandler;
        this.taskProducer = taskProducer;
    }

    /**
     * Consume tasks from the task.execute topic
     * Consumer group: taskforge-workers (allows multiple workers to process in parallel)
     * 
     * @param task The task to execute
     * @param partition The Kafka partition this message came from
     * @param offset The message offset
     */
    @KafkaListener(
            topics = KafkaConfig.TASK_EXECUTE_TOPIC,
            groupId = "taskforge-workers",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeTask(
            @Payload Task task,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset) {

        String taskId = task.getId().toString();
        
        log.info("========================================");
        log.info("Worker received task from Kafka");
        log.info("Task ID: {}", taskId);
        log.info("Task Name: {}", task.getName());
        log.info("Task Type: {}", task.getTaskType());
        log.info("Priority: {}", task.getPriority());
        log.info("Kafka Partition: {}", partition);
        log.info("Kafka Offset: {}", offset);
        log.info("========================================");

        // Try to acquire distributed lock
        boolean lockAcquired = lockManager.acquireLock(task.getId());
        
        if (!lockAcquired) {
            log.warn("🔒 Could not acquire lock for task: {}. Another worker may be processing it. Skipping.", taskId);
            log.warn("This is expected behavior in multi-worker setup and prevents duplicate execution.");
            return; // Skip this task, another worker is handling it
        }

        log.info("🔓 Lock acquired for task: {}", taskId);
        LocalDateTime executionStartTime = LocalDateTime.now();
        TaskExecutor.ExecutionResult result = null;
        
        try {
            // Update status to RUNNING
            updateTaskStatus(task, Task.TaskStatus.RUNNING);
            
            // Execute the task using TaskExecutor
            result = taskExecutor.execute(task);
            
            LocalDateTime executionEndTime = LocalDateTime.now();
            
            if (result.isSuccess()) {
                // Mark as SUCCESS
                updateTaskStatus(task, Task.TaskStatus.SUCCESS, result.getOutput());
                log.info("✅ Task completed successfully - ID: {}, Name: {}", taskId, task.getName());
            } else {
                // Mark as FAILED and handle retry
                updateTaskStatus(task, Task.TaskStatus.FAILED, result.getErrorMessage());
                log.error("❌ Task execution failed - ID: {}, Error: {}", taskId, result.getErrorMessage());
                
                // Handle retry logic
                handleTaskFailure(task, result.getErrorMessage());
            }
            
            // Log execution to database
            try {
                int attemptNumber = task.getRetryCount() + 1;
                executionLogService.logExecution(task, result, executionStartTime, executionEndTime, attemptNumber);
            } catch (Exception logEx) {
                log.error("Failed to save execution log - ID: {}, Error: {}", taskId, logEx.getMessage());
            }
            
        } catch (Exception e) {
            log.error("❌ Task execution exception - ID: {}, Error: {}", taskId, e.getMessage(), e);
            
            LocalDateTime executionEndTime = LocalDateTime.now();
            
            // Mark as FAILED
            updateTaskStatus(task, Task.TaskStatus.FAILED, e.getMessage());
            
            // Handle retry logic
            handleTaskFailure(task, e.getMessage());
            
            // Log failed execution
            try {
                TaskExecutor.ExecutionResult failureResult = TaskExecutor.ExecutionResult.failure(e.getMessage());
                int attemptNumber = task.getRetryCount() + 1;
                executionLogService.logExecution(task, failureResult, executionStartTime, executionEndTime, attemptNumber);
            } catch (Exception logEx) {
                log.error("Failed to save execution log for exception - ID: {}, Error: {}", taskId, logEx.getMessage());
            }
        } finally {
            // Always release the lock
            lockManager.releaseLock(task.getId());
            log.info("🔓 Lock released for task: {}", taskId);
        }
    }

    /**
     * Handle task failure with retry logic.
     * If retries remain, re-publishes to Kafka with exponential backoff.
     * If max retries exceeded, publishes to DLQ.
     * 
     * @param task Failed task
     * @param errorMessage Error message
     */
    private void handleTaskFailure(Task task, String errorMessage) {
        if (retryHandler.shouldRetry(task)) {
            // Prepare task for retry with exponential backoff
            Task retryTask = retryHandler.prepareForRetry(task, errorMessage);
            
            // Calculate delay and log retry info
            int delaySeconds = retryHandler.calculateBackoffDelay(retryTask.getRetryCount() - 1);
            log.info("🔄 Task {} will retry in {}s (attempt {}/{})", 
                    task.getId(), delaySeconds, retryTask.getRetryCount(), retryTask.getMaxRetries());
            
            // Re-publish to Kafka for retry
            // Note: scheduledAt is set to future time, SchedulerService will pick it up
            taskProducer.publishTask(retryTask);
            
        } else {
            // Max retries exceeded, prepare for DLQ
            Task deadTask = retryHandler.prepareForDLQ(task, errorMessage);
            log.error("💀 Task {} exceeded max retries ({}). Moving to DLQ.", 
                    task.getId(), task.getMaxRetries());
            
            // Publish to Dead Letter Queue
            taskProducer.publishToDeadLetterQueue(deadTask);
        }
    }

    /**
     * Update task status in the database
     * 
     * @param task The task to update
     * @param newStatus The new status
     */
    private void updateTaskStatus(Task task, Task.TaskStatus newStatus) {
        updateTaskStatus(task, newStatus, null);
    }

    /**
     * Update task status in the database with optional message
     * 
     * @param task The task to update
     * @param newStatus The new status
     * @param message Output or error message
     */
    private void updateTaskStatus(Task task, Task.TaskStatus newStatus, String message) {
        try {
            Task dbTask = taskRepository.findById(task.getId())
                    .orElseThrow(() -> new RuntimeException("Task not found: " + task.getId()));
            
            dbTask.setStatus(newStatus);
            
            // Set timestamps based on status
            if (newStatus == Task.TaskStatus.RUNNING) {
                dbTask.setStartedAt(LocalDateTime.now());
                log.info("⏱️ Task started - ID: {}", task.getId());
            } else if (newStatus == Task.TaskStatus.SUCCESS || newStatus == Task.TaskStatus.FAILED) {
                dbTask.setCompletedAt(LocalDateTime.now());
                
                if (message != null) {
                    if (newStatus == Task.TaskStatus.FAILED) {
                        dbTask.setErrorMessage(message);
                    }
                    // For SUCCESS, message could be logged but not stored (optional)
                    log.debug("Task {} - Message: {}", newStatus, message);
                }
            }
            
            taskRepository.save(dbTask);
            log.debug("Status updated - ID: {}, New Status: {}", task.getId(), newStatus);
            
        } catch (Exception e) {
            log.error("Failed to update task status - ID: {}, Status: {}, Error: {}", 
                    task.getId(), newStatus, e.getMessage(), e);
        }
    }

    /**
     * Handle Dead Letter Queue messages
     * Consumes permanently failed tasks for monitoring/alerting
     * 
     * @param task The failed task
     * @param partition Kafka partition
     * @param offset Message offset
     */
    @KafkaListener(
            topics = KafkaConfig.TASK_DEAD_LETTER_TOPIC,
            groupId = "taskforge-dlq-monitor",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeDeadLetterQueue(
            @Payload Task task,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset) {

        log.warn("========================================");
        log.warn("⚠️ DEAD LETTER QUEUE - Task permanently failed");
        log.warn("Task ID: {}", task.getId());
        log.warn("Task Name: {}", task.getName());
        log.warn("Retry Count: {}", task.getRetryCount());
        log.warn("Error: {}", task.getErrorMessage());
        log.warn("Partition: {}, Offset: {}", partition, offset);
        log.warn("========================================");

        // Update task status to DEAD in database
        try {
            Task dbTask = taskRepository.findById(task.getId())
                    .orElseThrow(() -> new RuntimeException("Task not found: " + task.getId()));
            
            dbTask.setStatus(Task.TaskStatus.DEAD);
            dbTask.setErrorMessage("Moved to DLQ after " + task.getRetryCount() + " retries");
            dbTask.setCompletedAt(LocalDateTime.now());
            
            taskRepository.save(dbTask);
            log.info("Task marked as DEAD in database - ID: {}", task.getId());
            
            // Future: Send alert/notification about permanently failed task
            
        } catch (Exception e) {
            log.error("Failed to update DLQ task status - ID: {}, Error: {}", 
                    task.getId(), e.getMessage(), e);
        }
    }
}
