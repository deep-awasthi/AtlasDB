package com.atlasdb.storage;

/**
 * Represents a key entry in the Custom Hash Table, holding the head of a version chain
 * for MVCC.
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public final class Entry<K, V> {

    private final K key;
    private volatile VersionNode<V> versionHead;
    private volatile Entry<K, V> next;

    /**
     * Constructs a new Entry.
     *
     * @param key         the entry key
     * @param versionHead the head of the version chain
     * @param next        the next entry in the collision bucket chain
     */
    public Entry(K key, VersionNode<V> versionHead, Entry<K, V> next) {
        this.key = key;
        this.versionHead = versionHead;
        this.next = next;
    }

    /**
     * Retrieves the key.
     *
     * @return the key
     */
    public K getKey() {
        return key;
    }

    /**
     * Retrieves the head node of the version chain.
     *
     * @return the head version node
     */
    public VersionNode<V> getVersionHead() {
        return versionHead;
    }

    /**
     * Sets the head node of the version chain.
     *
     * @param versionHead the new head version node
     */
    public void setVersionHead(VersionNode<V> versionHead) {
        this.versionHead = versionHead;
    }

    /**
     * Retrieves the next collision entry in the bucket list.
     *
     * @return the next entry
     */
    public Entry<K, V> getNext() {
        return next;
    }

    /**
     * Sets the next collision entry in the bucket list.
     *
     * @param next the next entry
     */
    public void setNext(Entry<K, V> next) {
        this.next = next;
    }

    /**
     * Compatibility helper to retrieve the most recent active value.
     *
     * @return the latest active value, or null if deleted or empty
     */
    public V getValue() {
        VersionNode<V> head = versionHead;
        return (head != null && !head.isDeleted()) ? head.getValue() : null;
    }

    /**
     * Compatibility helper to retrieve the most recent version timestamp.
     *
     * @return the latest version timestamp, or -1 if deleted or empty
     */
    public long getVersion() {
        VersionNode<V> head = versionHead;
        return head != null ? head.getTimestamp() : -1L;
    }
}
