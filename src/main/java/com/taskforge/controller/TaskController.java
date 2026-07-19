package com.taskforge.controller;

import com.taskforge.model.Task;
import com.taskforge.service.TaskService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

@RestController
@RequestMapping("/v1/tasks")
@Slf4j
public class TaskController {

    private final TaskService taskService;

    public TaskController(TaskService taskService) {
        this.taskService = taskService;
    }

    @PostMapping
    public ResponseEntity<Task> createTask(@RequestBody Task task) {
        log.info("REST: Create task request: {}", task.getName());
        Task createdTask = taskService.createTask(task);
        return ResponseEntity.status(HttpStatus.CREATED).body(createdTask);
    }

    @GetMapping("/{id}")
    public ResponseEntity<Task> getTaskById(@PathVariable UUID id) {
        log.info("REST: Get task by id: {}", id);
        Task task = taskService.getTaskById(id);
        return ResponseEntity.ok(task);
    }

    @GetMapping
    public ResponseEntity<Page<Task>> getAllTasks(
            @RequestParam(required = false) Task.TaskStatus status,
            Pageable pageable) {
        log.info("REST: Get all tasks, status: {}, page: {}", status, pageable.getPageNumber());
        
        Page<Task> tasks = status != null 
            ? taskService.getTasksByStatus(status, pageable)
            : taskService.getAllTasks(pageable);
        
        return ResponseEntity.ok(tasks);
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<Void> deleteTask(@PathVariable UUID id) {
        log.info("REST: Delete task: {}", id);
        taskService.deleteTask(id);
        return ResponseEntity.noContent().build();
    }

    @PatchMapping("/{id}/cancel")
    public ResponseEntity<Void> cancelTask(@PathVariable UUID id) {
        log.info("REST: Cancel task: {}", id);
        taskService.cancelTask(id);
        return ResponseEntity.ok().build();
    }
}
