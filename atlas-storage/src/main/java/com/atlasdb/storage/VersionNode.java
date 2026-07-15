package com.atlasdb.storage;

/**
 * Represents a single version node in an MVCC version chain.
 *
 * @param <V> the value type
 */
public final class VersionNode<V> {

    private final V value;
    private final long timestamp;
    private final boolean deleted;
    private volatile VersionNode<V> next;

    /**
     * Constructs a new VersionNode.
     *
     * @param value     the value payload
     * @param timestamp the version/timestamp when written
     * @param deleted   indicates if this is a tombstone
     * @param next      the next (older) version in the chain
     */
    public VersionNode(V value, long timestamp, boolean deleted, VersionNode<V> next) {
        this.value = value;
        this.timestamp = timestamp;
        this.deleted = deleted;
        this.next = next;
    }

    /**
     * Retrieves the value payload.
     *
     * @return the value
     */
    public V getValue() {
        return value;
    }

    /**
     * Retrieves the version/timestamp.
     *
     * @return the timestamp
     */
    public long getTimestamp() {
        return timestamp;
    }

    /**
     * Returns true if this is a tombstone (deleted record).
     *
     * @return true if deleted
     */
    public boolean isDeleted() {
        return deleted;
    }

    /**
     * Retrieves the next (older) version in the chain.
     *
     * @return the next version node, or null if this is the oldest version
     */
    public VersionNode<V> getNext() {
        return next;
    }

    /**
     * Sets the next older version in the chain.
     *
     * @param next the next version node
     */
    public void setNext(VersionNode<V> next) {
        this.next = next;
    }
}
