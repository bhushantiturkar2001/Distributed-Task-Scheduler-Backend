package com.taskforge.service;

import com.taskforge.kafka.TaskProducer;
import com.taskforge.model.Task;
import com.taskforge.repository.TaskRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;

/**
 * SchedulerService - Polls database for due tasks and publishes them to Kafka
 * Runs every 5 seconds to check for tasks that need to be executed
 */
@Service
@Slf4j
public class SchedulerService {

    private final TaskRepository taskRepository;
    private final TaskProducer taskProducer;

    public SchedulerService(TaskRepository taskRepository, TaskProducer taskProducer) {
        this.taskRepository = taskRepository;
        this.taskProducer = taskProducer;
    }

    /**
     * Poll database every 5 seconds for tasks that are due for execution
     * - Finds PENDING tasks where scheduled_at <= now()
     * - Updates status to QUEUED
     * - Publishes to Kafka for worker execution
     * 
     * Fixed delay: waits 5 seconds after the previous execution completes
     */
    @Scheduled(fixedDelay = 5000, initialDelay = 10000)
    @Transactional
    public void schedulePendingTasks() {
        LocalDateTime now = LocalDateTime.now();
        
        log.debug("🔍 Scheduler running - checking for due tasks at {}", now);

        try {
            // Find all PENDING tasks that are due for execution
            List<Task> dueTasks = taskRepository.findDueTasks(now);

            if (dueTasks.isEmpty()) {
                log.debug("No due tasks found");
                return;
            }

            log.info("📋 Found {} due task(s) ready for execution", dueTasks.size());

            int successCount = 0;
            int failureCount = 0;

            // Process each due task
            for (Task task : dueTasks) {
                try {
                    // Update status to QUEUED (task is being sent to Kafka)
                    task.setStatus(Task.TaskStatus.QUEUED);
                    taskRepository.save(task);

                    // Publish to Kafka for worker execution
                    taskProducer.publishTask(task);

                    successCount++;
                    log.info("✅ Task queued successfully - ID: {}, Name: {}, Priority: {}", 
                            task.getId(), task.getName(), task.getPriority());

                } catch (Exception e) {
                    failureCount++;
                    log.error("❌ Failed to queue task - ID: {}, Error: {}", 
                            task.getId(), e.getMessage(), e);
                    
                    // Revert status back to PENDING on failure
                    task.setStatus(Task.TaskStatus.PENDING);
                    task.setErrorMessage("Failed to queue: " + e.getMessage());
                    taskRepository.save(task);
                }
            }

            log.info("📊 Scheduling cycle complete - Success: {}, Failed: {}", 
                    successCount, failureCount);

        } catch (Exception e) {
            log.error("❌ Scheduler error: {}", e.getMessage(), e);
        }
    }

    /**
     * Get statistics about pending and queued tasks
     * Useful for monitoring scheduler health
     * 
     * @return Summary string with task counts
     */
    public String getSchedulerStats() {
        try {
            long pendingCount = taskRepository.countByStatus(Task.TaskStatus.PENDING);
            long queuedCount = taskRepository.countByStatus(Task.TaskStatus.QUEUED);
            long runningCount = taskRepository.countByStatus(Task.TaskStatus.RUNNING);

            return String.format(
                    "Scheduler Stats - PENDING: %d, QUEUED: %d, RUNNING: %d",
                    pendingCount, queuedCount, runningCount
            );
        } catch (Exception e) {
            log.error("Failed to get scheduler stats: {}", e.getMessage());
            return "Stats unavailable";
        }
    }

    /**
     * Manually trigger the scheduler (for testing/admin purposes)
     * Can be called via a REST endpoint or admin interface
     */
    public void triggerSchedulerManually() {
        log.info("⚡ Manual scheduler trigger initiated");
        schedulePendingTasks();
    }
}
