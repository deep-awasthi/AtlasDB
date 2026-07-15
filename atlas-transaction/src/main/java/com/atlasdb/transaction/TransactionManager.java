package com.atlasdb.transaction;

import com.atlasdb.storage.HashStorageEngine;
import com.atlasdb.storage.TimestampManager;
import com.atlasdb.transaction.lock.DeadlockDetector;
import com.atlasdb.transaction.lock.LockManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Main manager orchestrating transaction lifecycles, OCC validation,
 * lock management, and automatic deadlock resolution.
 */
public final class TransactionManager implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(TransactionManager.class);

    private final HashStorageEngine<String, String> storageEngine;
    private final TimestampManager timestampManager;
    private final LockManager lockManager;
    private final DeadlockDetector deadlockDetector;

    private final AtomicLong txnIdGenerator = new AtomicLong(0);
    private final Map<Long, Transaction> activeTransactions = new ConcurrentHashMap<>();

    /**
     * Constructs a TransactionManager.
     *
     * @param storageEngine    the database storage engine
     * @param timestampManager the active timestamp/version manager
     */
    public TransactionManager(HashStorageEngine<String, String> storageEngine, TimestampManager timestampManager) {
        if (storageEngine == null) {
            throw new IllegalArgumentException("Storage engine cannot be null.");
        }
        if (timestampManager == null) {
            throw new IllegalArgumentException("Timestamp manager cannot be null.");
        }
        this.storageEngine = storageEngine;
        this.timestampManager = timestampManager;
        this.lockManager = new LockManager();
        
        // Start deadlock detector scanning every 200 milliseconds
        this.deadlockDetector = new DeadlockDetector(this.lockManager, this, 200);
        this.deadlockDetector.start();
    }

    public HashStorageEngine<String, String> getStorageEngine() {
        return storageEngine;
    }

    public TimestampManager getTimestampManager() {
        return timestampManager;
    }

    public LockManager getLockManager() {
        return lockManager;
    }

    /**
     * Begins a new transaction with specified isolation level and concurrency mode.
     *
     * @param isolationLevel the isolation level
     * @param concurrencyMode the concurrency mode (OPTIMISTIC or PESSIMISTIC)
     * @return the active Transaction context
     */
    public Transaction begin(Transaction.IsolationLevel isolationLevel, Transaction.ConcurrencyMode concurrencyMode) {
        long txnId = txnIdGenerator.incrementAndGet();
        long startTimestamp = timestampManager.getNewTimestamp();
        
        // Register snapshot reader to protect versions from garbage collection
        timestampManager.registerReader(startTimestamp);

        Transaction txn = new Transaction(txnId, startTimestamp, isolationLevel, concurrencyMode, this, lockManager);
        activeTransactions.put(txnId, txn);
        
        logger.debug("Transaction {} started. Isolation: {}, Concurrency: {}", txnId, isolationLevel, concurrencyMode);
        return txn;
    }

    /**
     * Commits the transaction, validating OCC sets, applying modifications,
     * and releasing locks.
     *
     * @param txnId the transaction ID to commit
     */
    public void commit(long txnId) {
        Transaction txn = activeTransactions.get(txnId);
        if (txn == null) {
            throw new IllegalArgumentException("Transaction " + txnId + " not found or already completed.");
        }

        synchronized (txn) {
            if (txn.getState() != Transaction.State.ACTIVE) {
                throw new IllegalStateException("Transaction " + txnId + " is not active (State: " + txn.getState() + ").");
            }

            // 1. Validation phase for OCC
            if (txn.getConcurrencyMode() == Transaction.ConcurrencyMode.OPTIMISTIC && !txn.getWriteSet().isEmpty()) {
                for (String key : txn.getReadSet()) {
                    // Check if key has been modified by another committed transaction after startTimestamp
                    long latestVersion = storageEngine.getVersion(key);
                    if (latestVersion > txn.getStartTimestamp()) {
                        logger.warn("OCC Write Conflict detected on key '{}' for transaction {}. Latest version: {}, Txn start: {}",
                                key, txnId, latestVersion, txn.getStartTimestamp());
                        abortInternal(txn);
                        throw new IllegalStateException("OCC Validation failed. Write conflict on key: " + key);
                    }
                }
            }

            // 2. Commit phase: write buffered values to storage engine under a new commit timestamp
            long commitTimestamp = timestampManager.getNewTimestamp();
            for (Map.Entry<String, String> entry : txn.getWriteBuffer().entrySet()) {
                String key = entry.getKey();
                String val = entry.getValue();

                if (Transaction.TOMBSTONE.equals(val)) {
                    storageEngine.delete(key, commitTimestamp);
                } else {
                    storageEngine.put(key, val, commitTimestamp);
                }
            }

            txn.setState(Transaction.State.COMMITTED);
            
            // Release resources
            lockManager.releaseAll(txn.getLockedKeys(), txnId);
            timestampManager.deregisterReader(txn.getStartTimestamp());
            activeTransactions.remove(txnId);

            logger.debug("Transaction {} committed successfully at timestamp {}", txnId, commitTimestamp);
        }
    }

    /**
     * Aborts (rolls back) the transaction, abandoning modifications and releasing locks.
     *
     * @param txnId the transaction ID
     */
    public void abort(long txnId) {
        Transaction txn = activeTransactions.get(txnId);
        if (txn == null) {
            return;
        }

        synchronized (txn) {
            if (txn.getState() == Transaction.State.ACTIVE) {
                abortInternal(txn);
            }
        }
    }

    private void abortInternal(Transaction txn) {
        txn.setState(Transaction.State.ABORTED);
        lockManager.releaseAll(txn.getLockedKeys(), txn.getTxnId());
        timestampManager.deregisterReader(txn.getStartTimestamp());
        activeTransactions.remove(txn.getTxnId());
        logger.debug("Transaction {} aborted.", txn.getTxnId());
    }

    @Override
    public void close() {
        deadlockDetector.close();
        // Abort all active transactions on shutdown
        for (Transaction txn : activeTransactions.values()) {
            abort(txn.getTxnId());
        }
    }
}
