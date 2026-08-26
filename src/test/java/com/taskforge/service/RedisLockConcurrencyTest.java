package com.taskforge.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Concurrency test for RedisLockManager.
 * Simulates multiple workers competing for the same lock.
 */
@ExtendWith(MockitoExtension.class)
class RedisLockConcurrencyTest {

    private static final Logger log = LoggerFactory.getLogger(RedisLockConcurrencyTest.class);

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

    /**
     * Test: Multiple threads attempting to acquire the same lock
     * Only ONE thread should succeed (simulates multiple workers)
     */
    @Test
    void multipleThreads_OnlyOneAcquiresLock() throws InterruptedException {
        int numberOfThreads = 5;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch completionLatch = new CountDownLatch(numberOfThreads);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failureCount = new AtomicInteger(0);

        // First thread succeeds, others fail
        when(rLock.tryLock(anyLong(), anyLong(), any(TimeUnit.class)))
                .thenReturn(true)  // First call succeeds
                .thenReturn(false) // Subsequent calls fail
                .thenReturn(false)
                .thenReturn(false)
                .thenReturn(false);

        when(rLock.isHeldByCurrentThread()).thenReturn(true);

        ExecutorService executor = Executors.newFixedThreadPool(numberOfThreads);

        // Simulate 5 workers trying to acquire lock simultaneously
        for (int i = 0; i < numberOfThreads; i++) {
            final int workerId = i + 1;
            executor.submit(() -> {
                try {
                    startLatch.await(); // Wait for all threads to be ready

                    log.info("Worker {} attempting to acquire lock", workerId);
                    boolean acquired = lockManager.acquireLock(testTaskId);

                    if (acquired) {
                        successCount.incrementAndGet();
                        log.info("✅ Worker {} ACQUIRED lock", workerId);
                        
                        // Simulate work
                        Thread.sleep(50);
                        
                        lockManager.releaseLock(testTaskId);
                        log.info("Worker {} released lock", workerId);
                    } else {
                        failureCount.incrementAndGet();
                        log.warn("❌ Worker {} FAILED to acquire lock", workerId);
                    }
                } catch (Exception e) {
                    log.error("Worker {} error: {}", workerId, e.getMessage());
                } finally {
                    completionLatch.countDown();
                }
            });
        }

        // Start all workers simultaneously
        startLatch.countDown();

        // Wait for completion
        boolean completed = completionLatch.await(5, TimeUnit.SECONDS);
        executor.shutdown();

        // Assert
        assertTrue(completed, "All workers should complete");
        assertEquals(1, successCount.get(), "Exactly ONE worker should acquire lock");
        assertEquals(4, failureCount.get(), "Four workers should fail to acquire lock");

        log.info("Test result: {} successful, {} failed", successCount.get(), failureCount.get());
    }

    /**
     * Test: executeWithLock prevents concurrent execution
     */
    @Test
    void executeWithLock_OnlyOneThreadExecutes() throws InterruptedException {
        int numberOfThreads = 3;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch completionLatch = new CountDownLatch(numberOfThreads);
        AtomicInteger executionCount = new AtomicInteger(0);
        AtomicInteger skippedCount = new AtomicInteger(0);

        // First thread gets lock, others don't
        when(rLock.tryLock(anyLong(), anyLong(), any(TimeUnit.class)))
                .thenReturn(true)
                .thenReturn(false)
                .thenReturn(false);

        when(rLock.isHeldByCurrentThread()).thenReturn(true);

        ExecutorService executor = Executors.newFixedThreadPool(numberOfThreads);

        for (int i = 0; i < numberOfThreads; i++) {
            final int workerId = i + 1;
            executor.submit(() -> {
                try {
                    startLatch.await();

                    boolean executed = lockManager.executeWithLock(testTaskId, () -> {
                        log.info("Worker {} EXECUTING task", workerId);
                        executionCount.incrementAndGet();
                        
                        try {
                            Thread.sleep(100);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    });

                    if (!executed) {
                        skippedCount.incrementAndGet();
                        log.info("Worker {} SKIPPED execution", workerId);
                    }
                } catch (Exception e) {
                    log.error("Worker {} error: {}", workerId, e.getMessage());
                } finally {
                    completionLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        boolean completed = completionLatch.await(5, TimeUnit.SECONDS);
        executor.shutdown();

        // Assert
        assertTrue(completed);
        assertEquals(1, executionCount.get(), "Task should execute exactly ONCE");
        assertEquals(2, skippedCount.get(), "Two workers should skip");

        log.info("Execution: {} executed, {} skipped", executionCount.get(), skippedCount.get());
    }

    /**
     * Test: Lock is always released even if exception occurs
     */
    @Test
    void executeWithLock_ReleasesLockOnException() throws InterruptedException {
        when(rLock.tryLock(anyLong(), anyLong(), any(TimeUnit.class))).thenReturn(true);
        when(rLock.isHeldByCurrentThread()).thenReturn(true);

        // Execute task that throws exception
        assertThrows(RuntimeException.class, () -> {
            lockManager.executeWithLock(testTaskId, () -> {
                throw new RuntimeException("Simulated failure");
            });
        });

        // Verify unlock was called despite exception
        verify(rLock).unlock();
    }

    /**
     * Test: Sequential lock acquisitions work correctly
     */
    @Test
    void sequentialLockAcquisitions_WorkCorrectly() throws InterruptedException {
        when(rLock.tryLock(anyLong(), anyLong(), any(TimeUnit.class))).thenReturn(true);
        when(rLock.isHeldByCurrentThread()).thenReturn(true);

        // First acquisition
        assertTrue(lockManager.acquireLock(testTaskId));
        lockManager.releaseLock(testTaskId);

        // Second acquisition (after release)
        assertTrue(lockManager.acquireLock(testTaskId));
        lockManager.releaseLock(testTaskId);

        // Third acquisition
        assertTrue(lockManager.acquireLock(testTaskId));
        lockManager.releaseLock(testTaskId);

        // Verify lock/unlock called 3 times each
        verify(rLock, times(3)).tryLock(anyLong(), anyLong(), any(TimeUnit.class));
        verify(rLock, times(3)).unlock();
    }

    /**
     * Test: Different tasks can have locks simultaneously
     */
    @Test
    void differentTasks_CanBeLocked_Simultaneously() throws InterruptedException {
        UUID task1 = UUID.randomUUID();
        UUID task2 = UUID.randomUUID();
        UUID task3 = UUID.randomUUID();

        when(rLock.tryLock(anyLong(), anyLong(), any(TimeUnit.class))).thenReturn(true);
        when(rLock.isHeldByCurrentThread()).thenReturn(true);

        // Acquire locks for different tasks
        assertTrue(lockManager.acquireLock(task1));
        assertTrue(lockManager.acquireLock(task2));
        assertTrue(lockManager.acquireLock(task3));

        // All should succeed (different lock keys)
        verify(rLock, times(3)).tryLock(anyLong(), anyLong(), any(TimeUnit.class));

        // Release all
        lockManager.releaseLock(task1);
        lockManager.releaseLock(task2);
        lockManager.releaseLock(task3);

        verify(rLock, times(3)).unlock();
    }

    /**
     * Test: Rapid sequential lock attempts
     */
    @Test
    void rapidSequentialLockAttempts_HandleCorrectly() throws InterruptedException {
        when(rLock.tryLock(anyLong(), anyLong(), any(TimeUnit.class))).thenReturn(true);
        when(rLock.isHeldByCurrentThread()).thenReturn(true);

        int attempts = 10;
        for (int i = 0; i < attempts; i++) {
            assertTrue(lockManager.acquireLock(testTaskId), "Attempt " + i + " should succeed");
            lockManager.releaseLock(testTaskId);
        }

        verify(rLock, times(attempts)).tryLock(anyLong(), anyLong(), any(TimeUnit.class));
        verify(rLock, times(attempts)).unlock();
    }

    /**
     * Test: Thread interruption during lock acquisition
     */
    @Test
    void threadInterruption_HandledGracefully() throws InterruptedException {
        when(rLock.tryLock(anyLong(), anyLong(), any(TimeUnit.class)))
                .thenThrow(new InterruptedException("Thread interrupted"));

        // Should return false, not throw exception
        assertFalse(lockManager.acquireLock(testTaskId));
        
        // Unlock should not be called if lock wasn't acquired
        verify(rLock, never()).unlock();
    }
}
