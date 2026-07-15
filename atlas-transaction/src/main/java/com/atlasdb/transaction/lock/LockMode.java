package com.atlasdb.transaction.lock;

/**
 * Represents the locking modes available for transactions.
 */
public enum LockMode {
    /**
     * Shared lock mode (multiple transactions can read concurrently).
     */
    READ,

    /**
     * Exclusive lock mode (only one transaction can write/read).
     */
    WRITE
}
