package com.taskforge.service;

import com.taskforge.config.KafkaConfig;
import com.taskforge.kafka.TaskProducer;
import com.taskforge.model.Task;
import com.taskforge.repository.TaskRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.LocalDateTime;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

/**
 * Unit tests for Priority Queue functionality.
 * Verifies that tasks are routed to correct priority topics.
 */
@ExtendWith(MockitoExtension.class)
class PriorityQueueTest {

    @Mock
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Mock
    private TaskRepository taskRepository;

    @InjectMocks
    private TaskProducer taskProducer;

    private Task highPriorityTask;
    private Task mediumPriorityTask;
    private Task lowPriorityTask;

    @BeforeEach
    void setUp() {
        // Mock successful Kafka send
        CompletableFuture<SendResult<String, Object>> future = CompletableFuture.completedFuture(null);
        when(kafkaTemplate.send(any(String.class), any(String.class), any(Task.class)))
                .thenReturn(future);

        // Create test tasks with different priorities
        highPriorityTask = Task.builder()
                .id(UUID.randomUUID())
                .name("High Priority Task")
                .description("Urgent task")
                .payload("{\"action\":\"urgent\"}")
                .taskType(Task.TaskType.LOG)
                .priority(Task.TaskPriority.HIGH)
                .status(Task.TaskStatus.PENDING)
                .scheduledAt(LocalDateTime.now())
                .retryCount(0)
                .maxRetries(3)
                .build();

        mediumPriorityTask = Task.builder()
                .id(UUID.randomUUID())
                .name("Medium Priority Task")
                .description("Normal task")
                .payload("{\"action\":\"normal\"}")
                .taskType(Task.TaskType.LOG)
                .priority(Task.TaskPriority.MEDIUM)
                .status(Task.TaskStatus.PENDING)
                .scheduledAt(LocalDateTime.now())
                .retryCount(0)
                .maxRetries(3)
                .build();

        lowPriorityTask = Task.builder()
                .id(UUID.randomUUID())
                .name("Low Priority Task")
                .description("Background task")
                .payload("{\"action\":\"background\"}")
                .taskType(Task.TaskType.LOG)
                .priority(Task.TaskPriority.LOW)
                .status(Task.TaskStatus.PENDING)
                .scheduledAt(LocalDateTime.now())
                .retryCount(0)
                .maxRetries(3)
                .build();
    }

    @Test
    @DisplayName("High priority task should be routed to high priority topic")
    void testHighPriorityTaskRouting() {
        // When
        taskProducer.publishTask(highPriorityTask);

        // Then
        ArgumentCaptor<String> topicCaptor = ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate).send(
                topicCaptor.capture(),
                eq(highPriorityTask.getId().toString()),
                eq(highPriorityTask)
        );

        assertEquals(KafkaConfig.TASK_EXECUTE_HIGH_PRIORITY, topicCaptor.getValue(),
                "High priority task should be sent to task.execute.high topic");
    }

    @Test
    @DisplayName("Medium priority task should be routed to medium priority topic")
    void testMediumPriorityTaskRouting() {
        // When
        taskProducer.publishTask(mediumPriorityTask);

        // Then
        ArgumentCaptor<String> topicCaptor = ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate).send(
                topicCaptor.capture(),
                eq(mediumPriorityTask.getId().toString()),
                eq(mediumPriorityTask)
        );

        assertEquals(KafkaConfig.TASK_EXECUTE_MEDIUM_PRIORITY, topicCaptor.getValue(),
                "Medium priority task should be sent to task.execute.medium topic");
    }

    @Test
    @DisplayName("Low priority task should be routed to low priority topic")
    void testLowPriorityTaskRouting() {
        // When
        taskProducer.publishTask(lowPriorityTask);

        // Then
        ArgumentCaptor<String> topicCaptor = ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate).send(
                topicCaptor.capture(),
                eq(lowPriorityTask.getId().toString()),
                eq(lowPriorityTask)
        );

        assertEquals(KafkaConfig.TASK_EXECUTE_LOW_PRIORITY, topicCaptor.getValue(),
                "Low priority task should be sent to task.execute.low topic");
    }

    @Test
    @DisplayName("Null priority should default to medium priority topic")
    void testNullPriorityDefaultsToMedium() {
        // Given
        Task taskWithNullPriority = Task.builder()
                .id(UUID.randomUUID())
                .name("Task with null priority")
                .payload("{\"action\":\"test\"}")
                .taskType(Task.TaskType.LOG)
                .priority(null) // Null priority
                .status(Task.TaskStatus.PENDING)
                .scheduledAt(LocalDateTime.now())
                .retryCount(0)
                .maxRetries(3)
                .build();

        // When
        taskProducer.publishTask(taskWithNullPriority);

        // Then
        ArgumentCaptor<String> topicCaptor = ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate).send(
                topicCaptor.capture(),
                eq(taskWithNullPriority.getId().toString()),
                eq(taskWithNullPriority)
        );

        assertEquals(KafkaConfig.TASK_EXECUTE_MEDIUM_PRIORITY, topicCaptor.getValue(),
                "Null priority should default to medium priority topic");
    }

    @Test
    @DisplayName("Multiple tasks with different priorities should route correctly")
    void testMultiplePriorityTasksRouting() {
        // When - publish all three tasks
        taskProducer.publishTask(highPriorityTask);
        taskProducer.publishTask(mediumPriorityTask);
        taskProducer.publishTask(lowPriorityTask);

        // Then - verify all were sent to correct topics
        verify(kafkaTemplate, times(3)).send(
                any(String.class),
                any(String.class),
                any(Task.class)
        );

        ArgumentCaptor<String> topicCaptor = ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate, times(3)).send(
                topicCaptor.capture(),
                any(String.class),
                any(Task.class)
        );

        var capturedTopics = topicCaptor.getAllValues();
        assertTrue(capturedTopics.contains(KafkaConfig.TASK_EXECUTE_HIGH_PRIORITY),
                "Should contain high priority topic");
        assertTrue(capturedTopics.contains(KafkaConfig.TASK_EXECUTE_MEDIUM_PRIORITY),
                "Should contain medium priority topic");
        assertTrue(capturedTopics.contains(KafkaConfig.TASK_EXECUTE_LOW_PRIORITY),
                "Should contain low priority topic");
    }

    @Test
    @DisplayName("Task priority should be preserved after routing")
    void testPriorityPreservedAfterRouting() {
        // When
        taskProducer.publishTask(highPriorityTask);

        // Then
        ArgumentCaptor<Task> taskCaptor = ArgumentCaptor.forClass(Task.class);
        verify(kafkaTemplate).send(
                any(String.class),
                any(String.class),
                taskCaptor.capture()
        );

        Task capturedTask = taskCaptor.getValue();
        assertEquals(Task.TaskPriority.HIGH, capturedTask.getPriority(),
                "Task priority should be preserved after routing");
    }

    @Test
    @DisplayName("Task ID should be used as Kafka message key")
    void testTaskIdUsedAsMessageKey() {
        // When
        taskProducer.publishTask(highPriorityTask);

        // Then
        ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate).send(
                any(String.class),
                keyCaptor.capture(),
                any(Task.class)
        );

        assertEquals(highPriorityTask.getId().toString(), keyCaptor.getValue(),
                "Task ID should be used as Kafka message key for consistent partitioning");
    }
}
