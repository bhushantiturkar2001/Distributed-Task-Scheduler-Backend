package com.taskforge.dto;

import com.taskforge.model.Task;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.UUID;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class TaskResponse {

    private UUID id;
    private String name;
    private String description;
    private String payload;
    private Task.TaskType taskType;
    private Task.TaskPriority priority;
    private Task.TaskStatus status;
    private LocalDateTime scheduledAt;
    private LocalDateTime startedAt;
    private LocalDateTime completedAt;
    private Integer retryCount;
    private Integer maxRetries;
    private String errorMessage;
    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;

    public static TaskResponse fromEntity(Task task) {
        return TaskResponse.builder()
                .id(task.getId())
                .name(task.getName())
                .description(task.getDescription())
                .payload(task.getPayload())
                .taskType(task.getTaskType())
                .priority(task.getPriority())
                .status(task.getStatus())
                .scheduledAt(task.getScheduledAt())
                .startedAt(task.getStartedAt())
                .completedAt(task.getCompletedAt())
                .retryCount(task.getRetryCount())
                .maxRetries(task.getMaxRetries())
                .errorMessage(task.getErrorMessage())
                .createdAt(task.getCreatedAt())
                .updatedAt(task.getUpdatedAt())
                .build();
    }
}
