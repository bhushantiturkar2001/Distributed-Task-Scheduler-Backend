package com.taskforge.controller;

import com.taskforge.dto.CreateTaskRequest;
import com.taskforge.dto.TaskResponse;
import com.taskforge.dto.UpdateTaskRequest;
import com.taskforge.model.Task;
import com.taskforge.service.TaskService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
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
@Tag(name = "Task Management", description = "APIs for managing scheduled tasks")
public class TaskController {

    private final TaskService taskService;

    public TaskController(TaskService taskService) {
        this.taskService = taskService;
    }

    @PostMapping
    @Operation(summary = "Create a new task", description = "Schedule a new task for execution")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "201", description = "Task created successfully"),
        @ApiResponse(responseCode = "400", description = "Invalid input")
    })
    public ResponseEntity<TaskResponse> createTask(@Valid @RequestBody CreateTaskRequest request) {
        log.info("REST: Create task request: {}", request.getName());
        Task task = request.toEntity();
        Task createdTask = taskService.createTask(task);
        return ResponseEntity.status(HttpStatus.CREATED).body(TaskResponse.fromEntity(createdTask));
    }

    @GetMapping("/{id}")
    @Operation(summary = "Get task by ID", description = "Retrieve task details by its unique identifier")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Task found"),
        @ApiResponse(responseCode = "404", description = "Task not found")
    })
    public ResponseEntity<TaskResponse> getTaskById(
            @Parameter(description = "Task ID") @PathVariable UUID id) {
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
