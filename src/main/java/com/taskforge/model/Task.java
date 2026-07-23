package com.taskforge.model;

import jakarta.persistence.*;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import java.time.LocalDateTime;
import java.util.UUID;

@Entity
@Table(name = "tasks", indexes = {
    @Index(name = "idx_tasks_status_scheduled", columnList = "status,scheduled_at"),
    @Index(name = "idx_tasks_priority", columnList = "priority")
})
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Task {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @NotBlank(message = "Task name is required")
    @Column(nullable = false, length = 255)
    private String name;

    @Column(columnDefinition = "TEXT")
    private String description;

    @NotBlank(message = "Payload is required")
    @Column(nullable = false, columnDefinition = "jsonb")
    private String payload;

    @NotNull(message = "Task type is required")
    @Column(nullable = false, length = 50)
    @Enumerated(EnumType.STRING)
    private TaskType taskType;

    @Column(nullable = false, length = 10)
    @Enumerated(EnumType.STRING)
    @Builder.Default
    private TaskPriority priority = TaskPriority.MEDIUM;

    @Column(nullable = false, length = 20)
    @Enumerated(EnumType.STRING)
    @Builder.Default
    private TaskStatus status = TaskStatus.PENDING;

    @NotNull(message = "Scheduled time is required")
    @Column(nullable = false)
    private LocalDateTime scheduledAt;

    @Column
    private LocalDateTime startedAt;

    @Column
    private LocalDateTime completedAt;

    @Positive(message = "Retry count must be positive")
    @Column(nullable = false)
    @Builder.Default
    private Integer retryCount = 0;

    @Positive(message = "Max retries must be positive")
    @Column(nullable = false)
    @Builder.Default
    private Integer maxRetries = 3;

    @Column(columnDefinition = "TEXT")
    private String errorMessage;

    @CreationTimestamp
    @Column(nullable = false, updatable = false)
    private LocalDateTime createdAt;

    @UpdateTimestamp
    @Column(nullable = false)
    private LocalDateTime updatedAt;

    public enum TaskStatus {
        PENDING, QUEUED, RUNNING, SUCCESS, FAILED, RETRYING, DEAD, CANCELLED
    }

    public enum TaskType {
        HTTP_CALL, LOG, CUSTOM
    }

    public enum TaskPriority {
        HIGH, MEDIUM, LOW
    }
}
