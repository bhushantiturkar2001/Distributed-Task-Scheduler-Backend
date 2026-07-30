package com.taskforge.dto;

import com.taskforge.model.Task;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class UpdateTaskRequest {

    @NotBlank(message = "Task name is required")
    private String name;

    private String description;

    @NotBlank(message = "Payload is required")
    private String payload;

    @NotNull(message = "Task type is required")
    private Task.TaskType taskType;

    private Task.TaskPriority priority;

    @NotNull(message = "Scheduled time is required")
    private LocalDateTime scheduledAt;

    @Positive(message = "Max retries must be positive")
    private Integer maxRetries;
}
