package com.atlasdb.transaction.lock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;

/**
 * Key-level lock manager supporting Shared (READ) and Exclusive (WRITE) locks.
 * Loom-friendly, blocking waiting requests via CompletableFuture.
 * Exposes wait-for dependencies to resolve deadlocks.
 */
public final class LockManager {

    private static final Logger logger = LoggerFactory.getLogger(LockManager.class);

    // Map of Key -> KeyLock state
    private final ConcurrentHashMap<String, KeyLock> locks = new ConcurrentHashMap<>();

    private static final class LockRequest {
        final long txnId;
        final LockMode mode;
        final CompletableFuture<Boolean> granted = new CompletableFuture<>();

        LockRequest(long txnId, LockMode mode) {
            this.txnId = txnId;
            this.mode = mode;
        }
    }

    private static final class KeyLock {
        final String key;
        final Set<Long> holders = new HashSet<>();
        final Queue<LockRequest> waiters = new LinkedList<>();
        LockMode activeMode = null;

        KeyLock(String key) {
            this.key = key;
        }
    }

    /**
     * Acquires a lock on a specific key for the given transaction.
     * Blocks if the lock is held by another transaction, until granted or timeout occurs.
     *
     * @param key       the key to lock
     * @param txnId     the transaction ID requesting the lock
     * @param mode      the lock mode (READ or WRITE)
     * @param timeoutMs lock wait timeout in milliseconds
     * @return true if the lock was acquired, false on timeout
     * @throws InterruptedException if thread is interrupted while waiting
     */
    public boolean acquire(String key, long txnId, LockMode mode, long timeoutMs) throws InterruptedException {
        KeyLock lock = locks.computeIfAbsent(key, KeyLock::new);
        LockRequest request = null;

        synchronized (lock) {
            // 1. Re-entrant/upgrade checks
            if (lock.holders.contains(txnId)) {
                if (lock.activeMode == LockMode.WRITE || mode == LockMode.READ) {
                    return true; // Already holds sufficient lock
                }
                if (lock.holders.size() == 1) {
                    // Lock upgrade READ -> WRITE (only holder)
                    lock.activeMode = LockMode.WRITE;
                    return true;
                }
            }

            // 2. Try lock acquisition
            if (lock.activeMode == null) {
                lock.activeMode = mode;
                lock.holders.add(txnId);
                return true;
            }

            if (lock.activeMode == LockMode.READ && mode == LockMode.READ && lock.waiters.isEmpty()) {
                lock.holders.add(txnId);
                return true;
            }

            // 3. Queue blocking request
            request = new LockRequest(txnId, mode);
            lock.waiters.add(request);
        }

        // Wait outside synchronized block to prevent blocking other threads on KeyLock
        try {
            return request.granted.get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            // Remove request on timeout
            synchronized (lock) {
                lock.waiters.remove(request);
                // Trigger release logic in case this request was partially granted
                checkAndGrantWaiters(lock);
            }
            logger.warn("Timeout acquiring lock on key '{}' for transaction {}", key, txnId);
            return false;
        } catch (ExecutionException e) {
            logger.error("Execution exception in lock grant future", e);
            return false;
        }
    }

    /**
     * Releases the lock on a key held by the given transaction.
     *
     * @param key   the locked key
     * @param txnId the transaction ID releasing the lock
     */
    public void release(String key, long txnId) {
        KeyLock lock = locks.get(key);
        if (lock == null) {
            return;
        }

        synchronized (lock) {
            if (!lock.holders.remove(txnId)) {
                return;
            }

            if (lock.holders.isEmpty()) {
                lock.activeMode = null;
                checkAndGrantWaiters(lock);
            }
        }
    }

    /**
     * Releases locks on multiple keys held by the transaction.
     *
     * @param keys  the list of keys
     * @param txnId the transaction ID
     */
    public void releaseAll(Collection<String> keys, long txnId) {
        for (String key : keys) {
            release(key, txnId);
        }
    }

    private void checkAndGrantWaiters(KeyLock lock) {
        if (lock.activeMode != null || lock.waiters.isEmpty()) {
            return;
        }

        LockRequest first = lock.waiters.peek();
        if (first == null) {
            return;
        }

        if (first.mode == LockMode.WRITE) {
            // Grant exclusive write lock
            lock.waiters.poll();
            lock.activeMode = LockMode.WRITE;
            lock.holders.add(first.txnId);
            first.granted.complete(true);
        } else {
            // Grant shared read locks to all consecutive readers in queue
            lock.activeMode = LockMode.READ;
            while (!lock.waiters.isEmpty() && lock.waiters.peek().mode == LockMode.READ) {
                LockRequest readerReq = lock.waiters.poll();
                lock.holders.add(readerReq.txnId);
                readerReq.granted.complete(true);
            }
        }
    }

    /**
     * Builds and returns the Wait-For Graph representation of active lock contentions.
     * Edge T1 -> T2 indicates T1 is waiting for a lock held by T2.
     *
     * @return the wait-for dependency graph map
     */
    public Map<Long, Set<Long>> getWaitForGraph() {
        Map<Long, Set<Long>> graph = new HashMap<>();

        for (KeyLock lock : locks.values()) {
            synchronized (lock) {
                if (lock.holders.isEmpty() || lock.waiters.isEmpty()) {
                    continue;
                }

                for (LockRequest waiter : lock.waiters) {
                    Set<Long> dependencies = graph.computeIfAbsent(waiter.txnId, k -> new HashSet<>());
                    for (Long holder : lock.holders) {
                        if (!holder.equals(waiter.txnId)) {
                            dependencies.add(holder);
                        }
                    }
                }
            }
        }

        return graph;
    }
}
