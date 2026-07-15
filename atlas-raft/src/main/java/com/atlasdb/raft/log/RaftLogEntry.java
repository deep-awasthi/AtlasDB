package com.atlasdb.raft.log;

import java.util.Objects;

/**
 * Log entry representing a Raft replication command.
 */
public record RaftLogEntry(
        long index,
        long term,
        String key,
        String value,
        boolean isDelete
) {
    public RaftLogEntry {
        Objects.requireNonNull(key, "key cannot be null");
        if (index < 0) {
            throw new IllegalArgumentException("Raft log index must be non-negative");
        }
        if (term < 0) {
            throw new IllegalArgumentException("Raft term must be non-negative");
        }
    }
}
