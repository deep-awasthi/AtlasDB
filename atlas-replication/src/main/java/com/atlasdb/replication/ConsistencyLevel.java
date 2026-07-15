package com.atlasdb.replication;

/**
 * Consistency Levels for replication group read and write operations.
 */
public enum ConsistencyLevel {
    /**
     * Succeeds as soon as at least one replica completes the operation.
     */
    ONE,

    /**
     * Succeeds when a majority of active group replicas (floor(N / 2) + 1)
     * successfully complete the operation.
     */
    QUORUM,

    /**
     * Succeeds only when all active group replicas successfully complete the operation.
     */
    ALL
}
