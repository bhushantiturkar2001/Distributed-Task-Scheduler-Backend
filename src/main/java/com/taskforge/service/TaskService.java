package com.taskforge.service;

import com.taskforge.exception.InvalidTaskOperationException;
import com.taskforge.exception.TaskNotFoundException;
import com.taskforge.model.Task;
import com.taskforge.repository.TaskRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

@Service
@Slf4j
public class TaskService {

    private final TaskRepository taskRepository;

    public TaskService(TaskRepository taskRepository) {
        this.taskRepository = taskRepository;
    }

    @Transactional
    public Task createTask(Task task) {
        log.info("Creating new task: {}", task.getName());
        task.setStatus(Task.TaskStatus.PENDING);
        task.setRetryCount(0);
        return taskRepository.save(task);
    }

    @Transactional(readOnly = true)
    public Task getTaskById(UUID id) {
        log.debug("Fetching task with id: {}", id);
        return taskRepository.findById(id)
                .orElseThrow(() -> new TaskNotFoundException(id));
    }

    @Transactional(readOnly = true)
    public Page<Task> getAllTasks(Pageable pageable) {
        log.debug("Fetching all tasks, page: {}", pageable.getPageNumber());
        return taskRepository.findAll(pageable);
    }

    @Transactional(readOnly = true)
    public Page<Task> getTasksByStatus(Task.TaskStatus status, Pageable pageable) {
        log.debug("Fetching tasks by status: {}", status);
        return taskRepository.findByStatus(status, pageable);
    }

    @Transactional(readOnly = true)
    public List<Task> getDueTasks(LocalDateTime now) {
        log.debug("Fetching due tasks at: {}", now);
        return taskRepository.findDueTasks(now);
    }

    @Transactional(readOnly = true)
    public long getTaskCountByStatus(Task.TaskStatus status) {
        return taskRepository.countByStatus(status);
    }

    @Transactional
    public Task updateTask(UUID id, Task taskDetails) {
        log.info("Updating task: {}", id);
        Task task = getTaskById(id);
        
        if (task.getStatus() != Task.TaskStatus.PENDING) {
            throw new InvalidTaskOperationException(
                "Cannot update task that is not in PENDING status. Current status: " + task.getStatus());
        }

        task.setName(taskDetails.getName());
        task.setDescription(taskDetails.getDescription());
        task.setPayload(taskDetails.getPayload());
        task.setTaskType(taskDetails.getTaskType());
        task.setPriority(taskDetails.getPriority());
        task.setScheduledAt(taskDetails.getScheduledAt());
        task.setMaxRetries(taskDetails.getMaxRetries());

        return taskRepository.save(task);
    }

    @Transactional
    public void deleteTask(UUID id) {
        log.info("Deleting task: {}", id);
        Task task = getTaskById(id);
        
        if (task.getStatus() == Task.TaskStatus.RUNNING) {
            throw new InvalidTaskOperationException(
                "Cannot delete task that is currently running. Task ID: " + id);
        }

        taskRepository.deleteById(id);
    }

    @Transactional
    public void cancelTask(UUID id) {
        log.info("Cancelling task: {}", id);
        Task task = getTaskById(id);
        
        if (task.getStatus() == Task.TaskStatus.RUNNING || task.getStatus() == Task.TaskStatus.DEAD) {
            throw new InvalidTaskOperationException(
                "Cannot cancel task in current status: " + task.getStatus() + ". Task ID: " + id);
        }

        task.setStatus(Task.TaskStatus.CANCELLED);
        taskRepository.save(task);
    }

    // ============================================
    // Task Status Transition Methods
    // ============================================

    /**
     * Transition task from PENDING to QUEUED
     * Called by SchedulerService when publishing to Kafka
     * 
     * @param taskId The task ID
     * @return Updated task
     */
    @Transactional
    public Task transitionToQueued(UUID taskId) {
        log.debug("Transitioning task to QUEUED: {}", taskId);
        Task task = getTaskById(taskId);
        
        if (task.getStatus() != Task.TaskStatus.PENDING) {
            throw new InvalidTaskOperationException(
                "Can only transition to QUEUED from PENDING. Current status: " + task.getStatus());
        }
        
        task.setStatus(Task.TaskStatus.QUEUED);
        return taskRepository.save(task);
    }

    /**
     * Transition task from QUEUED to RUNNING
     * Called by TaskConsumer when starting execution
     * 
     * @param taskId The task ID
     * @return Updated task
     */
    @Transactional
    public Task transitionToRunning(UUID taskId) {
        log.debug("Transitioning task to RUNNING: {}", taskId);
        Task task = getTaskById(taskId);
        
        if (task.getStatus() != Task.TaskStatus.QUEUED && task.getStatus() != Task.TaskStatus.RETRYING) {
            throw new InvalidTaskOperationException(
                "Can only transition to RUNNING from QUEUED or RETRYING. Current status: " + task.getStatus());
        }
        
        task.setStatus(Task.TaskStatus.RUNNING);
        task.setStartedAt(LocalDateTime.now());
        return taskRepository.save(task);
    }

    /**
     * Transition task from RUNNING to SUCCESS
     * Called by TaskConsumer when execution succeeds
     * 
     * @param taskId The task ID
     * @return Updated task
     */
    @Transactional
    public Task transitionToSuccess(UUID taskId) {
        log.debug("Transitioning task to SUCCESS: {}", taskId);
        Task task = getTaskById(taskId);
        
        if (task.getStatus() != Task.TaskStatus.RUNNING) {
            throw new InvalidTaskOperationException(
                "Can only transition to SUCCESS from RUNNING. Current status: " + task.getStatus());
        }
        
        task.setStatus(Task.TaskStatus.SUCCESS);
        task.setCompletedAt(LocalDateTime.now());
        task.setErrorMessage(null); // Clear any previous errors
        return taskRepository.save(task);
    }

    /**
     * Transition task from RUNNING to FAILED
     * Called by TaskConsumer when execution fails
     * 
     * @param taskId The task ID
     * @param errorMessage Error details
     * @return Updated task
     */
    @Transactional
    public Task transitionToFailed(UUID taskId, String errorMessage) {
        log.debug("Transitioning task to FAILED: {}", taskId);
        Task task = getTaskById(taskId);
        
        if (task.getStatus() != Task.TaskStatus.RUNNING) {
            throw new InvalidTaskOperationException(
                "Can only transition to FAILED from RUNNING. Current status: " + task.getStatus());
        }
        
        task.setStatus(Task.TaskStatus.FAILED);
        task.setCompletedAt(LocalDateTime.now());
        task.setErrorMessage(errorMessage);
        return taskRepository.save(task);
    }

    /**
     * Transition task from FAILED to RETRYING
     * Called by retry handler when task will be retried
     * 
     * @param taskId The task ID
     * @return Updated task
     */
    @Transactional
    public Task transitionToRetrying(UUID taskId) {
        log.debug("Transitioning task to RETRYING: {}", taskId);
        Task task = getTaskById(taskId);
        
        if (task.getStatus() != Task.TaskStatus.FAILED) {
            throw new InvalidTaskOperationException(
                "Can only transition to RETRYING from FAILED. Current status: " + task.getStatus());
        }
        
        // Check retry limit
        if (task.getRetryCount() >= task.getMaxRetries()) {
            throw new InvalidTaskOperationException(
                "Task has exceeded max retries: " + task.getRetryCount() + "/" + task.getMaxRetries());
        }
        
        task.setStatus(Task.TaskStatus.RETRYING);
        task.setRetryCount(task.getRetryCount() + 1);
        return taskRepository.save(task);
    }

    /**
     * Transition task to DEAD (permanently failed)
     * Called by retry handler when max retries exceeded
     * 
     * @param taskId The task ID
     * @return Updated task
     */
    @Transactional
    public Task transitionToDead(UUID taskId) {
        log.warn("Transitioning task to DEAD: {}", taskId);
        Task task = getTaskById(taskId);
        
        task.setStatus(Task.TaskStatus.DEAD);
        task.setCompletedAt(LocalDateTime.now());
        
        if (task.getErrorMessage() == null) {
            task.setErrorMessage("Task permanently failed after " + task.getRetryCount() + " retries");
        }
        
        return taskRepository.save(task);
    }

    /**
     * Validate if a status transition is allowed
     * 
     * @param currentStatus Current task status
     * @param newStatus Target status
     * @return true if transition is valid
     */
    public boolean isValidTransition(Task.TaskStatus currentStatus, Task.TaskStatus newStatus) {
        return switch (newStatus) {
            case QUEUED -> currentStatus == Task.TaskStatus.PENDING;
            case RUNNING -> currentStatus == Task.TaskStatus.QUEUED || currentStatus == Task.TaskStatus.RETRYING;
            case SUCCESS, FAILED -> currentStatus == Task.TaskStatus.RUNNING;
            case RETRYING -> currentStatus == Task.TaskStatus.FAILED;
            case DEAD -> currentStatus == Task.TaskStatus.FAILED;
            case CANCELLED -> currentStatus != Task.TaskStatus.RUNNING && currentStatus != Task.TaskStatus.DEAD;
            default -> false;
        };
    }
}
