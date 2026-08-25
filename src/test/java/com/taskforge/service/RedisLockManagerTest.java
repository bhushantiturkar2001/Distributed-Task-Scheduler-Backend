package com.taskforge.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for RedisLockManager
 * Tests distributed lock acquisition, release, and edge cases
 */
@ExtendWith(MockitoExtension.class)
class RedisLockManagerTest {

    @Mock
    private RedissonClient redissonClient;

    @Mock
    private RLock rLock;

    @InjectMocks
    private RedisLockManager lockManager;

    private UUID testTaskId;

    @BeforeEach
    void setUp() {
        testTaskId = UUID.randomUUID();
        when(redissonClient.getLock(anyString())).thenReturn(rLock);
    }

    @Test
    void acquireLock_Success() throws InterruptedException {
        // Arrange
        when(rLock.tryLock(anyLong(), anyLong(), any(TimeUnit.class))).thenReturn(true);

        // Act
        boolean acquired = lockManager.acquireLock(testTaskId);

        // Assert
        assertTrue(acquired);
        verify(rLock).tryLock(anyLong(), anyLong(), eq(TimeUnit.SECONDS));
    }

    @Test
    void acquireLock_Failure() throws InterruptedException {
        // Arrange
        when(rLock.tryLock(anyLong(), anyLong(), any(TimeUnit.class))).thenReturn(false);

        // Act
        boolean acquired = lockManager.acquireLock(testTaskId);

        // Assert
        assertFalse(acquired);
        verify(rLock).tryLock(anyLong(), anyLong(), eq(TimeUnit.SECONDS));
    }

    @Test
    void acquireLock_InterruptedException() throws InterruptedException {
        // Arrange
        when(rLock.tryLock(anyLong(), anyLong(), any(TimeUnit.class)))
                .thenThrow(new InterruptedException("Thread interrupted"));

        // Act
        boolean acquired = lockManager.acquireLock(testTaskId);

        // Assert
        assertFalse(acquired);
        verify(rLock).tryLock(anyLong(), anyLong(), eq(TimeUnit.SECONDS));
    }

    @Test
    void releaseLock_WhenHeldByCurrentThread() {
        // Arrange
        when(rLock.isHeldByCurrentThread()).thenReturn(true);

        // Act
        lockManager.releaseLock(testTaskId);

        // Assert
        verify(rLock).isHeldByCurrentThread();
        verify(rLock).unlock();
    }

    @Test
    void releaseLock_WhenNotHeldByCurrentThread() {
        // Arrange
        when(rLock.isHeldByCurrentThread()).thenReturn(false);

        // Act
        lockManager.releaseLock(testTaskId);

        // Assert
        verify(rLock).isHeldByCurrentThread();
        verify(rLock, never()).unlock();
    }

    @Test
    void executeWithLock_Success() throws InterruptedException {
        // Arrange
        when(rLock.tryLock(anyLong(), anyLong(), any(TimeUnit.class))).thenReturn(true);
        when(rLock.isHeldByCurrentThread()).thenReturn(true);

        // Act
        String result = lockManager.executeWithLock(testTaskId, () -> "Success");

        // Assert
        assertEquals("Success", result);
        verify(rLock).tryLock(anyLong(), anyLong(), eq(TimeUnit.SECONDS));
        verify(rLock).unlock();
    }

    @Test
    void executeWithLock_LockNotAcquired() throws InterruptedException {
        // Arrange
        when(rLock.tryLock(anyLong(), anyLong(), any(TimeUnit.class))).thenReturn(false);

        // Act
        String result = lockManager.executeWithLock(testTaskId, () -> "Should not execute");

        // Assert
        assertNull(result);
        verify(rLock).tryLock(anyLong(), anyLong(), eq(TimeUnit.SECONDS));
        verify(rLock, never()).unlock();
    }

    @Test
    void executeWithLock_Runnable_Success() throws InterruptedException {
        // Arrange
        when(rLock.tryLock(anyLong(), anyLong(), any(TimeUnit.class))).thenReturn(true);
        when(rLock.isHeldByCurrentThread()).thenReturn(true);

        // Act
        boolean executed = lockManager.executeWithLock(testTaskId, () -> {
            // Simulate work
        });

        // Assert
        assertTrue(executed);
        verify(rLock).tryLock(anyLong(), anyLong(), eq(TimeUnit.SECONDS));
        verify(rLock).unlock();
    }

    @Test
    void executeWithLock_Runnable_LockNotAcquired() throws InterruptedException {
        // Arrange
        when(rLock.tryLock(anyLong(), anyLong(), any(TimeUnit.class))).thenReturn(false);

        // Act
        boolean executed = lockManager.executeWithLock(testTaskId, () -> {
            // Should not execute
        });

        // Assert
        assertFalse(executed);
        verify(rLock).tryLock(anyLong(), anyLong(), eq(TimeUnit.SECONDS));
        verify(rLock, never()).unlock();
    }

    @Test
    void isLocked_ReturnsTrue() {
        // Arrange
        when(rLock.isLocked()).thenReturn(true);

        // Act
        boolean locked = lockManager.isLocked(testTaskId);

        // Assert
        assertTrue(locked);
        verify(rLock).isLocked();
    }

    @Test
    void isLocked_ReturnsFalse() {
        // Arrange
        when(rLock.isLocked()).thenReturn(false);

        // Act
        boolean locked = lockManager.isLocked(testTaskId);

        // Assert
        assertFalse(locked);
        verify(rLock).isLocked();
    }

    @Test
    void isHeldByCurrentThread_ReturnsTrue() {
        // Arrange
        when(rLock.isHeldByCurrentThread()).thenReturn(true);

        // Act
        boolean held = lockManager.isHeldByCurrentThread(testTaskId);

        // Assert
        assertTrue(held);
        verify(rLock).isHeldByCurrentThread();
    }

    @Test
    void isHeldByCurrentThread_ReturnsFalse() {
        // Arrange
        when(rLock.isHeldByCurrentThread()).thenReturn(false);

        // Act
        boolean held = lockManager.isHeldByCurrentThread(testTaskId);

        // Assert
        assertFalse(held);
        verify(rLock).isHeldByCurrentThread();
    }

    @Test
    void getRemainingTtl_ReturnsValue() {
        // Arrange
        long expectedTtl = 30000L; // 30 seconds
        when(rLock.remainTimeToLive()).thenReturn(expectedTtl);

        // Act
        long actualTtl = lockManager.getRemainingTtl(testTaskId);

        // Assert
        assertEquals(expectedTtl, actualTtl);
        verify(rLock).remainTimeToLive();
    }

    @Test
    void forceUnlock_ExecutesSuccessfully() {
        // Act
        assertDoesNotThrow(() -> lockManager.forceUnlock(testTaskId));

        // Assert
        verify(rLock).forceUnlock();
    }

    @Test
    void acquireLock_WithCustomParameters() throws InterruptedException {
        // Arrange
        long waitTime = 10L;
        long ttl = 120L;
        when(rLock.tryLock(eq(waitTime), eq(ttl), any(TimeUnit.class))).thenReturn(true);

        // Act
        boolean acquired = lockManager.acquireLock(testTaskId, waitTime, ttl);

        // Assert
        assertTrue(acquired);
        verify(rLock).tryLock(eq(waitTime), eq(ttl), eq(TimeUnit.SECONDS));
    }
}
