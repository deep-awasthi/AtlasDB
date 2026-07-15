package com.atlasdb.storage;

import com.atlasdb.common.VersionGenerator;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Coordinates read/write timestamps and tracks active transaction readers.
 * Enables Snapshot Isolation by tracking the minimum active read timestamp.
 */
public final class TimestampManager {

    private final VersionGenerator versionGenerator;
    // Map of active read timestamp -> count of active readers at that timestamp
    private final ConcurrentSkipListMap<Long, Integer> activeReaders = new ConcurrentSkipListMap<>();

    /**
     * Constructs a TimestampManager.
     *
     * @param versionGenerator the monotonic version/timestamp generator
     */
    public TimestampManager(VersionGenerator versionGenerator) {
        if (versionGenerator == null) {
            throw new IllegalArgumentException("Version generator cannot be null.");
        }
        this.versionGenerator = versionGenerator;
    }

    /**
     * Allocates a new unique monotonic write timestamp.
     *
     * @return the write timestamp
     */
    public long getNewTimestamp() {
        return versionGenerator.nextVersion();
    }

    /**
     * Retrieves the current database version/timestamp.
     *
     * @return the current timestamp
     */
    public long currentTimestamp() {
        return versionGenerator.currentVersion();
    }

    /**
     * Registers an active reader transaction starting at the specified read timestamp.
     *
     * @param readTimestamp the transaction's read snapshot timestamp
     */
    public void registerReader(long readTimestamp) {
        activeReaders.merge(readTimestamp, 1, Integer::sum);
    }

    /**
     * Deregisters an active reader transaction upon completion.
     *
     * @param readTimestamp the transaction's read snapshot timestamp
     */
    public void deregisterReader(long readTimestamp) {
        activeReaders.computeIfPresent(readTimestamp, (ts, count) -> {
            int newCount = count - 1;
            return newCount <= 0 ? null : newCount;
        });
    }

    /**
     * Returns the minimum active read timestamp.
     * Any version older than this timestamp (which has a newer version succeeding it)
     * is no longer visible to any active reader and is eligible for garbage collection.
     *
     * @return the minimum active reader timestamp, or the current global timestamp if no readers are active
     */
    public long getMinActiveTimestamp() {
        Long first = activeReaders.isEmpty() ? null : activeReaders.firstKey();
        return first != null ? first : versionGenerator.currentVersion();
    }
}
