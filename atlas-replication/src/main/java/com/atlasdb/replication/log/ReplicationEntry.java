package com.atlasdb.replication.log;

import java.util.Objects;

/**
 * Log entry representing a replicated modification.
 */
public record ReplicationEntry(
        long lsn,
        String key,
        String value,
        long timestamp,
        boolean isDelete
) {
    public ReplicationEntry {
        Objects.requireNonNull(key, "key cannot be null");
        if (lsn < 0) {
            throw new IllegalArgumentException("lsn must be non-negative");
        }
    }
}
