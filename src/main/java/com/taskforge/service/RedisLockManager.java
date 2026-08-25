package com.taskforge.service;

import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Distributed lock manager using Redis (Redisson).
 * Prevents duplicate task execution across multiple worker instances.
 * 
 * Features:
 * - Automatic lock expiration (TTL)
 * - Lock attempt timeout
 * - Safe lock release
 * - Execute-with-lock pattern
 */
@Service
public class RedisLockManager {

    private static final Logger log = LoggerFactory.getLogger(RedisLockManager.class);

    private static final String LOCK_PREFIX = "taskforge:lock:task:";

    private final RedissonClient redissonClient;

    @Value("${taskforge.lock.ttl-seconds:60}")
    private long lockTtlSeconds;

    @Value("${taskforge.lock.wait-time-seconds:5}")
    private long lockWaitTimeSeconds;

    public RedisLockManager(RedissonClient redissonClient) {
        this.redissonClient = redissonClient;
    }

    /**
     * Acquire a distributed lock for a task.
     * 
     * @param taskId Task UUID
     * @return true if lock acquired, false otherwise
     */
    public boolean acquireLock(UUID taskId) {
        return acquireLock(taskId, lockWaitTimeSeconds, lockTtlSeconds);
    }

    /**
     * Acquire a distributed lock with custom timeout and TTL.
     * 
     * @param taskId Task UUID
     * @param waitTime Maximum time to wait for lock
     * @param ttl Lock expiration time (lease time)
     * @return true if lock acquired, false otherwise
     */
    public boolean acquireLock(UUID taskId, long waitTime, long ttl) {
        String lockKey = getLockKey(taskId);
        RLock lock = redissonClient.getLock(lockKey);

        try {
            boolean acquired = lock.tryLock(waitTime, ttl, TimeUnit.SECONDS);
            
            if (acquired) {
                log.info("Lock acquired for task: {} (TTL: {}s)", taskId, ttl);
            } else {
                log.warn("Failed to acquire lock for task: {} after waiting {}s", taskId, waitTime);
            }
            
            return acquired;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Lock acquisition interrupted for task: {}", taskId, e);
            return false;
        } catch (Exception e) {
            log.error("Error acquiring lock for task: {}", taskId, e);
            return false;
        }
    }

    /**
     * Release a distributed lock for a task.
     * 
     * @param taskId Task UUID
     */
    public void releaseLock(UUID taskId) {
        String lockKey = getLockKey(taskId);
        RLock lock = redissonClient.getLock(lockKey);

        try {
            // Only unlock if current thread holds the lock
            if (lock.isHeldByCurrentThread()) {
                lock.unlock();
                log.info("Lock released for task: {}", taskId);
            } else {
                log.debug("Lock not held by current thread for task: {}", taskId);
            }
        } catch (IllegalMonitorStateException e) {
            log.warn("Attempted to release lock not owned by current thread for task: {}", taskId);
        } catch (Exception e) {
            log.error("Error releasing lock for task: {}", taskId, e);
        }
    }

    /**
     * Execute a task with automatic lock acquisition and release.
     * This is the recommended pattern for using locks.
     * 
     * @param taskId Task UUID
     * @param action Action to execute while holding the lock
     * @param <T> Return type
     * @return Result of action, or null if lock not acquired
     */
    public <T> T executeWithLock(UUID taskId, Supplier<T> action) {
        if (!acquireLock(taskId)) {
            log.warn("Could not acquire lock for task: {}. Skipping execution.", taskId);
            return null;
        }

        try {
            log.debug("Executing action for task: {} with lock", taskId);
            return action.get();
        } catch (Exception e) {
            log.error("Error executing action for task: {}", taskId, e);
            throw e;
        } finally {
            releaseLock(taskId);
        }
    }

    /**
     * Execute a void task with automatic lock acquisition and release.
     * 
     * @param taskId Task UUID
     * @param action Action to execute while holding the lock
     * @return true if executed successfully, false if lock not acquired
     */
    public boolean executeWithLock(UUID taskId, Runnable action) {
        if (!acquireLock(taskId)) {
            log.warn("Could not acquire lock for task: {}. Skipping execution.", taskId);
            return false;
        }

        try {
            log.debug("Executing action for task: {} with lock", taskId);
            action.run();
            return true;
        } catch (Exception e) {
            log.error("Error executing action for task: {}", taskId, e);
            throw e;
        } finally {
            releaseLock(taskId);
        }
    }

    /**
     * Check if a lock is currently held for a task.
     * 
     * @param taskId Task UUID
     * @return true if locked, false otherwise
     */
    public boolean isLocked(UUID taskId) {
        String lockKey = getLockKey(taskId);
        RLock lock = redissonClient.getLock(lockKey);
        return lock.isLocked();
    }

    /**
     * Check if the current thread holds the lock for a task.
     * 
     * @param taskId Task UUID
     * @return true if current thread holds lock, false otherwise
     */
    public boolean isHeldByCurrentThread(UUID taskId) {
        String lockKey = getLockKey(taskId);
        RLock lock = redissonClient.getLock(lockKey);
        return lock.isHeldByCurrentThread();
    }

    /**
     * Force release a lock (use with caution).
     * This should only be used for manual cleanup or emergency scenarios.
     * 
     * @param taskId Task UUID
     */
    public void forceUnlock(UUID taskId) {
        String lockKey = getLockKey(taskId);
        RLock lock = redissonClient.getLock(lockKey);
        
        try {
            lock.forceUnlock();
            log.warn("Force unlocked task: {}", taskId);
        } catch (Exception e) {
            log.error("Error force unlocking task: {}", taskId, e);
        }
    }

    /**
     * Get remaining TTL for a lock.
     * 
     * @param taskId Task UUID
     * @return Remaining time in milliseconds, -1 if not locked, -2 if no expiration
     */
    public long getRemainingTtl(UUID taskId) {
        String lockKey = getLockKey(taskId);
        RLock lock = redissonClient.getLock(lockKey);
        return lock.remainTimeToLive();
    }

    /**
     * Generate Redis lock key for a task.
     */
    private String getLockKey(UUID taskId) {
        return LOCK_PREFIX + taskId.toString();
    }
}
