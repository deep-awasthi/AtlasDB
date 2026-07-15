package com.atlasdb.monitoring.jmx;

/**
 * Standard MBean interface exposing AtlasDB management and monitoring attributes/operations.
 */
public interface AtlasDbMonitorMBean {

    /**
     * Returns current number of active keys in the database.
     */
    int getDatabaseSize();

    /**
     * Returns count of active concurrent transactions.
     */
    long getActiveTransactions();

    /**
     * Returns count of successfully committed transactions.
     */
    long getCommittedTransactions();

    /**
     * Returns count of aborted/rolled back transactions.
     */
    long getAbortedTransactions();

    /**
     * Triggers active database pruning/garbage collection.
     */
    void triggerGarbageCollection();
}
