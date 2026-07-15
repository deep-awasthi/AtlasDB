package com.atlasdb.storage;

import java.util.Iterator;

/**
 * Core interface for storage engines in AtlasDB.
 * Defines the standard CRUD operations, version lookup, memory metrics, and iteration capabilities.
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public interface StorageEngine<K, V> extends Iterable<Entry<K, V>> {

    /**
     * Associates the specified value with the specified key.
     * If the key already exists, updates the value.
     *
     * @param key   key with which the specified value is to be associated
     * @param value value to be associated with the specified key
     * @return the monotonic version generated for this write operation
     */
    long put(K key, V value);

    /**
     * Associates the specified value with the specified key using a pre-allocated version.
     * Used by transactional subsystems committing pre-buffered writes.
     *
     * @param key     key with which the specified value is to be associated
     * @param value   value to be associated with the specified key
     * @param version the pre-allocated version timestamp
     * @return the committed version
     */
    long put(K key, V value, long version);

    /**
     * Returns the value to which the specified key is mapped.
     *
     * @param key the key whose associated value is to be returned
     * @return the value to which the specified key is mapped, or null if the key is not found
     */
    V get(K key);

    /**
     * Returns the version of the entry associated with the specified key.
     *
     * @param key the key whose associated version is to be returned
     * @return the version of the entry, or -1 if the key is not found
     */
    long getVersion(K key);

    /**
     * Removes the mapping for a key from this storage engine if it is present.
     *
     * @param key key whose mapping is to be removed from the storage engine
     * @return true if the key was found and removed, false otherwise
     */
    boolean delete(K key);

    /**
     * Deletes the mapping for a key from the storage engine with a pre-allocated version.
     * Used by transactional subsystems committing pre-buffered deletes.
     *
     * @param key     key whose mapping is to be removed
     * @param version the pre-allocated version timestamp
     * @return true if the key was active and tombstone was appended, false otherwise
     */
    boolean delete(K key, long version);

    /**
     * Returns true if this storage engine contains a mapping for the specified key.
     *
     * @param key key whose presence in this storage engine is to be tested
     * @return true if this storage engine contains a mapping for the specified key
     */
    boolean containsKey(K key);

    /**
     * Returns the number of key-value mappings in this storage engine.
     *
     * @return the number of key-value mappings in this storage engine
     */
    int size();

    /**
     * Removes all mappings from this storage engine.
     */
    void clear();

    /**
     * Returns the memory stats of the storage engine.
     *
     * @return memory statistics of this storage engine instance
     */
    MemoryStats getMemoryStats();

    /**
     * Returns an iterator over the entries in this storage engine.
     * The iterator must be thread-safe (weakly consistent).
     *
     * @return an iterator over the entries
     */
    @Override
    Iterator<Entry<K, V>> iterator();
}
