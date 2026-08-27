package com.taskforge.service;

import com.taskforge.model.Task;
import com.taskforge.repository.TaskRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.LocalDateTime;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Unit tests for RetryHandler - exponential backoff and retry logic
 */
@ExtendWith(MockitoExtension.class)
class RetryHandlerTest {

    @Mock
    private TaskRepository taskRepository;

    @InjectMocks
    private RetryHandler retryHandler;

    private Task testTask;

    @BeforeEach
    void setUp() {
        // Set configuration values
        ReflectionTestUtils.setField(retryHandler, "baseDelaySeconds", 1);
        ReflectionTestUtils.setField(retryHandler, "maxDelaySeconds", 300);

        testTask = Task.builder()
                .id(UUID.randomUUID())
                .name("Test Task")
                .retryCount(0)
                .maxRetries(3)
                .status(Task.TaskStatus.FAILED)
                .build();
    }

    @Test
    void shouldRetry_WithRetriesRemaining_ReturnsTrue() {
        // Arrange
        testTask.setRetryCount(1);
        testTask.setMaxRetries(3);

        // Act
        boolean result = retryHandler.shouldRetry(testTask);

        // Assert
        assertTrue(result);
    }

    @Test
    void shouldRetry_MaxRetriesReached_ReturnsFalse() {
        // Arrange
        testTask.setRetryCount(3);
        testTask.setMaxRetries(3);

        // Act
        boolean result = retryHandler.shouldRetry(testTask);

        // Assert
        assertFalse(result);
    }

    @Test
    void shouldRetry_MaxRetriesExceeded_ReturnsFalse() {
        // Arrange
        testTask.setRetryCount(5);
        testTask.setMaxRetries(3);

        // Act
        boolean result = retryHandler.shouldRetry(testTask);

        // Assert
        assertFalse(result);
    }

    @Test
    void calculateBackoffDelay_ExponentialProgression() {
        // Test exponential backoff: 1s, 2s, 4s, 8s, 16s
        assertEquals(1, retryHandler.calculateBackoffDelay(0));  // 1 * 2^0 = 1
        assertEquals(2, retryHandler.calculateBackoffDelay(1));  // 1 * 2^1 = 2
        assertEquals(4, retryHandler.calculateBackoffDelay(2));  // 1 * 2^2 = 4
        assertEquals(8, retryHandler.calculateBackoffDelay(3));  // 1 * 2^3 = 8
        assertEquals(16, retryHandler.calculateBackoffDelay(4)); // 1 * 2^4 = 16
        assertEquals(32, retryHandler.calculateBackoffDelay(5)); // 1 * 2^5 = 32
    }

    @Test
    void calculateBackoffDelay_CappedAtMaxDelay() {
        // Test that delay doesn't exceed maxDelaySeconds (300)
        assertEquals(300, retryHandler.calculateBackoffDelay(10)); // Would be 1024, capped at 300
    }

    @Test
    void calculateNextRetryTime_ReturnsCorrectFutureTime() {
        // Act
        LocalDateTime before = LocalDateTime.now();
        LocalDateTime nextRetry = retryHandler.calculateNextRetryTime(2); // 4 second delay
        LocalDateTime after = LocalDateTime.now().plusSeconds(4);

        // Assert
        assertTrue(nextRetry.isAfter(before));
        assertTrue(nextRetry.isBefore(after.plusSeconds(1))); // Allow 1s tolerance
    }

    @Test
    void prepareForRetry_FirstRetry_IncrementsCountAndUpdatesStatus() {
        // Arrange
        testTask.setRetryCount(0);
        when(taskRepository.save(any(Task.class))).thenAnswer(i -> i.getArguments()[0]);

        // Act
        Task result = retryHandler.prepareForRetry(testTask, "Connection timeout");

        // Assert
        assertEquals(1, result.getRetryCount());
        assertEquals(Task.TaskStatus.RETRYING, result.getStatus());
        assertNotNull(result.getScheduledAt());
        assertTrue(result.getErrorMessage().contains("Retry 1/3"));
        assertTrue(result.getErrorMessage().contains("Connection timeout"));
        verify(taskRepository).save(testTask);
    }

    @Test
    void prepareForRetry_SecondRetry_HasLongerDelay() {
        // Arrange
        testTask.setRetryCount(1);
        when(taskRepository.save(any(Task.class))).thenAnswer(i -> i.getArguments()[0]);

        // Act
        Task result = retryHandler.prepareForRetry(testTask, "Server error");

        // Assert
        assertEquals(2, result.getRetryCount());
        assertEquals(Task.TaskStatus.RETRYING, result.getStatus());
        assertTrue(result.getErrorMessage().contains("Retry 2/3"));
    }

    @Test
    void prepareForRetry_MaxRetriesExceeded_MovesToDLQ() {
        // Arrange
        testTask.setRetryCount(3);
        testTask.setMaxRetries(3);
        when(taskRepository.save(any(Task.class))).thenAnswer(i -> i.getArguments()[0]);

        // Act
        Task result = retryHandler.prepareForRetry(testTask, "Final error");

        // Assert
        assertEquals(Task.TaskStatus.DEAD, result.getStatus());
        assertTrue(result.getErrorMessage().contains("Max retries (3) exceeded"));
        assertTrue(result.getErrorMessage().contains("Final error"));
        assertNotNull(result.getCompletedAt());
    }

    @Test
    void prepareForDLQ_UpdatesStatusAndSavesTask() {
        // Arrange
        when(taskRepository.save(any(Task.class))).thenAnswer(i -> i.getArguments()[0]);

        // Act
        Task result = retryHandler.prepareForDLQ(testTask, "Permanent failure");

        // Assert
        assertEquals(Task.TaskStatus.DEAD, result.getStatus());
        assertTrue(result.getErrorMessage().contains("Max retries"));
        assertTrue(result.getErrorMessage().contains("Permanent failure"));
        assertNotNull(result.getCompletedAt());
        verify(taskRepository).save(testTask);
    }

    @Test
    void handleFailure_WithRetriesRemaining_PreparesForRetry() {
        // Arrange
        testTask.setRetryCount(1);
        when(taskRepository.findById(testTask.getId())).thenReturn(Optional.of(testTask));
        when(taskRepository.save(any(Task.class))).thenAnswer(i -> i.getArguments()[0]);

        // Act
        Task result = retryHandler.handleFailure(testTask.getId(), "Network error");

        // Assert
        assertEquals(Task.TaskStatus.RETRYING, result.getStatus());
        assertEquals(2, result.getRetryCount());
        verify(taskRepository).findById(testTask.getId());
        verify(taskRepository).save(testTask);
    }

    @Test
    void handleFailure_MaxRetriesExceeded_PreparesForDLQ() {
        // Arrange
        testTask.setRetryCount(3);
        when(taskRepository.findById(testTask.getId())).thenReturn(Optional.of(testTask));
        when(taskRepository.save(any(Task.class))).thenAnswer(i -> i.getArguments()[0]);

        // Act
        Task result = retryHandler.handleFailure(testTask.getId(), "Final error");

        // Assert
        assertEquals(Task.TaskStatus.DEAD, result.getStatus());
        assertTrue(result.getErrorMessage().contains("Max retries"));
        verify(taskRepository).findById(testTask.getId());
        verify(taskRepository).save(testTask);
    }

    @Test
    void getRetryStats_ReturnsCorrectStatistics() {
        // Arrange
        testTask.setRetryCount(2);
        testTask.setMaxRetries(5);

        // Act
        RetryHandler.RetryStats stats = retryHandler.getRetryStats(testTask);

        // Assert
        assertEquals(2, stats.getCurrentRetry());
        assertEquals(5, stats.getMaxRetries());
        assertEquals(3, stats.getRemainingRetries());
        assertEquals(4, stats.getNextDelaySeconds()); // 2^2 = 4
        assertTrue(stats.isCanRetry());
    }

    @Test
    void getRetryStats_MaxRetriesReached_ShowsNoRetry() {
        // Arrange
        testTask.setRetryCount(3);
        testTask.setMaxRetries(3);

        // Act
        RetryHandler.RetryStats stats = retryHandler.getRetryStats(testTask);

        // Assert
        assertEquals(3, stats.getCurrentRetry());
        assertEquals(3, stats.getMaxRetries());
        assertEquals(0, stats.getRemainingRetries());
        assertEquals(0, stats.getNextDelaySeconds());
        assertFalse(stats.isCanRetry());
    }

    @Test
    void shouldRetry_NullRetryCount_InitializesToZero() {
        // Arrange
        testTask.setRetryCount(null);
        testTask.setMaxRetries(3);

        // Act
        boolean result = retryHandler.shouldRetry(testTask);

        // Assert
        assertTrue(result);
        assertEquals(0, testTask.getRetryCount());
    }

    @Test
    void shouldRetry_NullMaxRetries_InitializesToThree() {
        // Arrange
        testTask.setRetryCount(0);
        testTask.setMaxRetries(null);

        // Act
        boolean result = retryHandler.shouldRetry(testTask);

        // Assert
        assertTrue(result);
        assertEquals(3, testTask.getMaxRetries());
    }
}
