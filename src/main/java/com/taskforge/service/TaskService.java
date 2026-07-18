package com.taskforge.service;

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
                .orElseThrow(() -> new RuntimeException("Task not found with id: " + id));
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
    public void deleteTask(UUID id) {
        log.info("Deleting task: {}", id);
        Task task = getTaskById(id);
        
        if (task.getStatus() == Task.TaskStatus.RUNNING) {
            throw new RuntimeException("Cannot delete task that is currently running");
        }

        taskRepository.deleteById(id);
    }

    @Transactional
    public void cancelTask(UUID id) {
        log.info("Cancelling task: {}", id);
        Task task = getTaskById(id);
        
        if (task.getStatus() == Task.TaskStatus.RUNNING || task.getStatus() == Task.TaskStatus.DEAD) {
            throw new RuntimeException("Cannot cancel task in current status: " + task.getStatus());
        }

        task.setStatus(Task.TaskStatus.CANCELLED);
        taskRepository.save(task);
    }
}
