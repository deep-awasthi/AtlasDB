package com.atlasdb.replication.log;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Thread-safe replication log capturing change history.
 */
public final class ReplicationLog {

    private final List<ReplicationEntry> entries = new ArrayList<>();
    private final AtomicLong lsnGenerator = new AtomicLong(0);
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    /**
     * Appends a new modification entry to the replication log.
     */
    public ReplicationEntry append(String key, String value, long timestamp, boolean isDelete) {
        lock.writeLock().lock();
        try {
            long lsn = lsnGenerator.incrementAndGet();
            ReplicationEntry entry = new ReplicationEntry(lsn, key, value, timestamp, isDelete);
            entries.add(entry);
            return entry;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Appends an existing entry (used by followers to sync replication logs).
     */
    public void appendEntry(ReplicationEntry entry) {
        lock.writeLock().lock();
        try {
            entries.add(entry);
            // Ensure LSN generator stays ahead of sync entries
            long currentLsn = lsnGenerator.get();
            if (entry.lsn() > currentLsn) {
                lsnGenerator.set(entry.lsn());
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Returns all log entries with a sequence number greater than the given LSN.
     */
    public List<ReplicationEntry> getEntriesSince(long lsn) {
        lock.readLock().lock();
        try {
            List<ReplicationEntry> result = new ArrayList<>();
            for (ReplicationEntry entry : entries) {
                if (entry.lsn() > lsn) {
                    result.add(entry);
                }
            }
            return result;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Gets the latest LSN generated in the log.
     */
    public long getLatestLsn() {
        return lsnGenerator.get();
    }
}
