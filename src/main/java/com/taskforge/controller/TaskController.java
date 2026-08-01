package com.taskforge.controller;

import com.taskforge.dto.CreateTaskRequest;
import com.taskforge.dto.TaskResponse;
import com.taskforge.dto.UpdateTaskRequest;
import com.taskforge.model.Task;
import com.taskforge.service.TaskService;
import jakarta.validation.Valid;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

@RestController
@RequestMapping("/v1/tasks")
@Slf4j
@Validated
public class TaskController {

    private final TaskService taskService;

    public TaskController(TaskService taskService) {
        this.taskService = taskService;
    }

    @PostMapping
    public ResponseEntity<TaskResponse> createTask(@Valid @RequestBody CreateTaskRequest request) {
        log.info("REST: Create task request: {}", request.getName());
        Task task = request.toEntity();
        Task createdTask = taskService.createTask(task);
        return ResponseEntity.status(HttpStatus.CREATED).body(TaskResponse.fromEntity(createdTask));
    }

    @GetMapping("/{id}")
    public ResponseEntity<TaskResponse> getTaskById(@PathVariable UUID id) {
        log.info("REST: Get task by id: {}", id);
        Task task = taskService.getTaskById(id);
        return ResponseEntity.ok(TaskResponse.fromEntity(task));
    }

    @GetMapping
    public ResponseEntity<Page<TaskResponse>> getAllTasks(
            @RequestParam(required = false) Task.TaskStatus status,
            Pageable pageable) {
        log.info("REST: Get all tasks, status: {}, page: {}", status, pageable.getPageNumber());
        
        Page<Task> tasks = status != null 
            ? taskService.getTasksByStatus(status, pageable)
            : taskService.getAllTasks(pageable);
        
        return ResponseEntity.ok(tasks.map(TaskResponse::fromEntity));
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

    @PutMapping("/{id}")
    public ResponseEntity<TaskResponse> updateTask(@PathVariable UUID id, @Valid @RequestBody UpdateTaskRequest request) {
        log.info("REST: Update task: {}", id);
        Task taskDetails = Task.builder()
                .name(request.getName())
                .description(request.getDescription())
                .payload(request.getPayload())
                .taskType(request.getTaskType())
                .priority(request.getPriority())
                .scheduledAt(request.getScheduledAt())
                .maxRetries(request.getMaxRetries())
                .build();
        Task updatedTask = taskService.updateTask(id, taskDetails);
        return ResponseEntity.ok(TaskResponse.fromEntity(updatedTask));
    }
}
