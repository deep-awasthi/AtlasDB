package com.atlasdb.transaction;

import com.atlasdb.storage.HashStorageEngine;
import com.atlasdb.transaction.lock.LockManager;
import com.atlasdb.transaction.lock.LockMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Encapsulates an active database transaction context.
 * Manages write buffering, read/write set tracking, and lock acquisition.
 */
public final class Transaction {

    private static final Logger logger = LoggerFactory.getLogger(Transaction.class);

    // Marker value to represent buffered deletes
    public static final String TOMBSTONE = "__ATLASDB_TXN_TOMBSTONE__";

    public enum State {
        ACTIVE,
        COMMITTED,
        ABORTED
    }

    public enum IsolationLevel {
        READ_COMMITTED,
        REPEATABLE_READ,
        SNAPSHOT_ISOLATION
    }

    public enum ConcurrencyMode {
        OPTIMISTIC,
        PESSIMISTIC
    }

    private final long txnId;
    private final long startTimestamp;
    private final IsolationLevel isolationLevel;
    private final ConcurrencyMode concurrencyMode;
    
    private final TransactionManager transactionManager;
    private final LockManager lockManager;

    private volatile State state = State.ACTIVE;
    private final String abortReason = null;

    // Buffer of local uncommitted writes: Key -> Value
    private final Map<String, String> writeBuffer = new ConcurrentHashMap<>();
    
    // Track keys read and written for validation and locking
    private final Set<String> readSet = ConcurrentHashMap.newKeySet();
    private final Set<String> writeSet = ConcurrentHashMap.newKeySet();

    // Track locks held by this transaction (pessimistic mode)
    private final Set<String> lockedKeys = ConcurrentHashMap.newKeySet();

    /**
     * Constructs a Transaction context.
     */
    public Transaction(long txnId, long startTimestamp, IsolationLevel isolationLevel,
                       ConcurrencyMode concurrencyMode, TransactionManager transactionManager,
                       LockManager lockManager) {
        this.txnId = txnId;
        this.startTimestamp = startTimestamp;
        this.isolationLevel = isolationLevel;
        this.concurrencyMode = concurrencyMode;
        this.transactionManager = transactionManager;
        this.lockManager = lockManager;
    }

    public long getTxnId() {
        return txnId;
    }

    public long getStartTimestamp() {
        return startTimestamp;
    }

    public IsolationLevel getIsolationLevel() {
        return isolationLevel;
    }

    public ConcurrencyMode getConcurrencyMode() {
        return concurrencyMode;
    }

    public HashStorageEngine<String, String> getStorageEngine() {
        return transactionManager.getStorageEngine();
    }

    public State getState() {
        return state;
    }

    public void setState(State state) {
        this.state = state;
    }

    public Map<String, String> getWriteBuffer() {
        return writeBuffer;
    }

    public Set<String> getReadSet() {
        return readSet;
    }

    public Set<String> getWriteSet() {
        return writeSet;
    }

    public Set<String> getLockedKeys() {
        return lockedKeys;
    }

    /**
     * Reads a value by key under transaction isolation visibility rules.
     *
     * @param key the target key
     * @return the value visible to the transaction, or null if deleted or absent
     */
    public String get(String key) {
        checkActive();
        
        // 1. Check local write buffer first
        if (writeBuffer.containsKey(key)) {
            String val = writeBuffer.get(key);
            return TOMBSTONE.equals(val) ? null : val;
        }

        // 2. Lock key if operating in pessimistic mode
        if (concurrencyMode == ConcurrencyMode.PESSIMISTIC) {
            acquireLockWithRetry(key, LockMode.READ);
        }

        // 3. Read from storage engine with snapshot read timestamp
        long readTs = (isolationLevel == IsolationLevel.READ_COMMITTED) 
                ? transactionManager.getTimestampManager().currentTimestamp() 
                : startTimestamp;

        String dbValue = transactionManager.getStorageEngine().get(key, readTs);
        
        // 4. Record to read set for OCC validation
        readSet.add(key);

        return dbValue;
    }

    /**
     * Writes a key-value pair, buffering the write locally.
     *
     * @param key   the key
     * @param value the value
     */
    public void put(String key, String value) {
        if (value == null) {
            throw new IllegalArgumentException("Value cannot be null. Use delete for removal.");
        }
        checkActive();

        if (concurrencyMode == ConcurrencyMode.PESSIMISTIC) {
            acquireLockWithRetry(key, LockMode.WRITE);
        }

        writeBuffer.put(key, value);
        writeSet.add(key);
    }

    /**
     * Deletes a key, buffering a tombstone locally.
     *
     * @param key the key
     */
    public void delete(String key) {
        checkActive();

        if (concurrencyMode == ConcurrencyMode.PESSIMISTIC) {
            acquireLockWithRetry(key, LockMode.WRITE);
        }

        writeBuffer.put(key, TOMBSTONE);
        writeSet.add(key);
    }

    /**
     * Commits this transaction, validating changes and writing to storage.
     */
    public void commit() {
        transactionManager.commit(txnId);
    }

    /**
     * Aborts (rolls back) this transaction, discarding changes and releasing locks.
     */
    public void abort() {
        transactionManager.abort(txnId);
    }

    private void checkActive() {
        if (state != State.ACTIVE) {
            throw new IllegalStateException("Transaction " + txnId + " is no longer active (State: " + state + ").");
        }
    }

    private void acquireLockWithRetry(String key, LockMode mode) {
        if (lockedKeys.contains(key)) {
            // Already holds write lock, or holds read lock and requests read lock
            return;
        }

        try {
            // Wait up to 5 seconds for lock acquisition
            boolean acquired = lockManager.acquire(key, txnId, mode, 5000);
            if (!acquired) {
                // Lock acquisition timeout: abort due to lock contention or potential cycle
                logger.warn("Transaction {} timed out acquiring lock on key '{}'. Aborting...", txnId, key);
                abort();
                throw new IllegalStateException("Lock timeout on key: " + key);
            }
            lockedKeys.add(key);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            abort();
            throw new IllegalStateException("Transaction lock acquisition interrupted.", ie);
        }
    }
}
