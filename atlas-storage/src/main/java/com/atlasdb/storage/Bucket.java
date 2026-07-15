package com.atlasdb.storage;

import java.util.concurrent.locks.StampedLock;

/**
 * A Bucket inside the Custom Hash Table.
 * Manages access concurrency using StampedLock and stores the head of the collision chain.
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public final class Bucket<K, V> {

    private final StampedLock lock = new StampedLock();
    private volatile Entry<K, V> head;

    /**
     * Retrieves the head entry of the chain.
     *
     * @return the head entry, or null if empty
     */
    public Entry<K, V> getHead() {
        return head;
    }

    /**
     * Sets the head entry of the chain.
     *
     * @param head the new head entry
     */
    public void setHead(Entry<K, V> head) {
        this.head = head;
    }

    /**
     * Acquires a read lock stamp.
     *
     * @return the lock stamp
     */
    public long readLock() {
        return lock.readLock();
    }

    /**
     * Releases a read lock.
     *
     * @param stamp the lock stamp to release
     */
    public void unlockRead(long stamp) {
        lock.unlockRead(stamp);
    }

    /**
     * Acquires a write lock stamp.
     *
     * @return the lock stamp
     */
    public long writeLock() {
        return lock.writeLock();
    }

    /**
     * Releases a write lock.
     *
     * @param stamp the lock stamp to release
     */
    public void unlockWrite(long stamp) {
        lock.unlockWrite(stamp);
    }

    /**
     * Tries to acquire an optimistic read stamp.
     *
     * @return the optimistic read stamp, or 0 if write locked
     */
    public long tryOptimisticRead() {
        return lock.tryOptimisticRead();
    }

    /**
     * Validates if the lock stamp is still valid (no write lock was acquired since stamp).
     *
     * @param stamp the optimistic read stamp
     * @return true if valid, false otherwise
     */
    public boolean validate(long stamp) {
        return lock.validate(stamp);
    }
}
