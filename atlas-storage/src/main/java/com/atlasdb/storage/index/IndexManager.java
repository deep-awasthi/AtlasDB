package com.atlasdb.storage.index;

import com.atlasdb.storage.Entry;
import com.atlasdb.storage.StorageEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Orchestrates Secondary, Range, Prefix, and TTL indexes for AtlasDB.
 * Integrates dynamic extraction of index keys and coordinates multi-threaded updates.
 * Expiry cleaning is conducted periodically by a background Loom virtual thread.
 */
public final class IndexManager implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(IndexManager.class);

    private final StorageEngine<String, String> storageEngine;
    private final AtomicBoolean running = new AtomicBoolean(false);

    // Index field name -> Map of AttributeValue -> Set of Primary Keys (Secondary & Range)
    private final ConcurrentSkipListMap<String, ConcurrentSkipListMap<String, Set<String>>> secondaryIndexes = new ConcurrentSkipListMap<>();

    // Index field name -> Prefix Trie (Prefix)
    private final ConcurrentSkipListMap<String, PrefixIndexTrie> prefixIndexes = new ConcurrentSkipListMap<>();

    // TTL Index: Expiration Epoch Time -> Set of Primary Keys
    private final ConcurrentSkipListMap<Long, Set<String>> ttlIndex = new ConcurrentSkipListMap<>();
    private final ConcurrentSkipListMap<String, Long> keyExpirations = new ConcurrentSkipListMap<>();

    private Thread ttlCleanerThread;

    /**
     * Constructs an IndexManager.
     *
     * @param storageEngine the storage engine to bind and clean expired keys from
     */
    public IndexManager(StorageEngine<String, String> storageEngine) {
        this.storageEngine = storageEngine;
    }

    /**
     * Starts the TTL cleaner background task on a Project Loom virtual thread.
     */
    public synchronized void start() {
        if (running.getAndSet(true)) {
            return;
        }

        ttlCleanerThread = Thread.ofVirtual()
                .name("atlasdb-ttl-cleaner")
                .unstarted(this::ttlCleanerLoop);
        ttlCleanerThread.start();
        logger.info("IndexManager background TTL cleaner started.");
    }

    /**
     * Registers a secondary field for indexing.
     *
     * @param fieldName the field name (e.g. "age" or "city")
     */
    public void registerIndex(String fieldName) {
        secondaryIndexes.putIfAbsent(fieldName, new ConcurrentSkipListMap<>());
    }

    /**
     * Registers a prefix trie index on a field.
     *
     * @param fieldName the field name
     */
    public void registerPrefixIndex(String fieldName) {
        prefixIndexes.putIfAbsent(fieldName, new PrefixIndexTrie());
    }

    /**
     * Updates all indexes during a write mutation (put/update).
     *
     * @param primaryKey the record's primary key
     * @param oldValue   the record's old value (null if new)
     * @param newValue   the record's new value
     */
    public void onPut(String primaryKey, String oldValue, String newValue) {
        // 1. Process standard Secondary/Range indexes
        for (Map.Entry<String, ConcurrentSkipListMap<String, Set<String>>> indexEntry : secondaryIndexes.entrySet()) {
            String field = indexEntry.getKey();
            ConcurrentSkipListMap<String, Set<String>> indexMap = indexEntry.getValue();

            String oldFieldVal = KeyValueParser.parseFieldValue(oldValue, field);
            String newFieldVal = KeyValueParser.parseFieldValue(newValue, field);

            if (oldFieldVal != null) {
                Set<String> pks = indexMap.get(oldFieldVal);
                if (pks != null) {
                    pks.remove(primaryKey);
                    if (pks.isEmpty()) {
                        indexMap.remove(oldFieldVal, Collections.emptySet());
                    }
                }
            }

            if (newFieldVal != null) {
                indexMap.computeIfAbsent(newFieldVal, k -> new CopyOnWriteArraySet<>()).add(primaryKey);
            }
        }

        // 2. Process Prefix Trie indexes
        for (Map.Entry<String, PrefixIndexTrie> entry : prefixIndexes.entrySet()) {
            String field = entry.getKey();
            PrefixIndexTrie trie = entry.getValue();

            String oldFieldVal = KeyValueParser.parseFieldValue(oldValue, field);
            String newFieldVal = KeyValueParser.parseFieldValue(newValue, field);

            if (oldFieldVal != null) {
                trie.remove(oldFieldVal, primaryKey);
            }
            if (newFieldVal != null) {
                trie.insert(newFieldVal, primaryKey);
            }
        }

        // 3. Process TTL index updates
        // If the record specifies a "ttl" attribute (in milliseconds), register expiration
        String ttlStr = KeyValueParser.parseFieldValue(newValue, "ttl");
        if (ttlStr != null) {
            try {
                long ttlMs = Long.parseLong(ttlStr);
                long expirationTime = System.currentTimeMillis() + ttlMs;

                // Remove old expiration if exists
                Long oldExpiration = keyExpirations.remove(primaryKey);
                if (oldExpiration != null) {
                    Set<String> keys = ttlIndex.get(oldExpiration);
                    if (keys != null) {
                        keys.remove(primaryKey);
                    }
                }

                // Register new expiration
                keyExpirations.put(primaryKey, expirationTime);
                ttlIndex.computeIfAbsent(expirationTime, k -> new CopyOnWriteArraySet<>()).add(primaryKey);
            } catch (NumberFormatException e) {
                logger.warn("Invalid TTL value '{}' for key '{}'", ttlStr, primaryKey);
            }
        } else {
            // No TTL in new value, remove old expiration if exists
            Long oldExpiration = keyExpirations.remove(primaryKey);
            if (oldExpiration != null) {
                Set<String> keys = ttlIndex.get(oldExpiration);
                if (keys != null) {
                    keys.remove(primaryKey);
                }
            }
        }
    }

    /**
     * Removes records from all indexes during a delete mutation.
     *
     * @param primaryKey the record's primary key
     * @param oldValue   the record's old value
     */
    public void onDelete(String primaryKey, String oldValue) {
        if (oldValue == null) {
            return;
        }

        // Remove from Secondary/Range indexes
        for (Map.Entry<String, ConcurrentSkipListMap<String, Set<String>>> indexEntry : secondaryIndexes.entrySet()) {
            String field = indexEntry.getKey();
            ConcurrentSkipListMap<String, Set<String>> indexMap = indexEntry.getValue();

            String oldFieldVal = KeyValueParser.parseFieldValue(oldValue, field);
            if (oldFieldVal != null) {
                Set<String> pks = indexMap.get(oldFieldVal);
                if (pks != null) {
                    pks.remove(primaryKey);
                }
            }
        }

        // Remove from Prefix Trie indexes
        for (Map.Entry<String, PrefixIndexTrie> entry : prefixIndexes.entrySet()) {
            String field = entry.getKey();
            PrefixIndexTrie trie = entry.getValue();

            String oldFieldVal = KeyValueParser.parseFieldValue(oldValue, field);
            if (oldFieldVal != null) {
                trie.remove(oldFieldVal, primaryKey);
            }
        }

        // Remove from TTL index
        Long expiration = keyExpirations.remove(primaryKey);
        if (expiration != null) {
            Set<String> keys = ttlIndex.get(expiration);
            if (keys != null) {
                keys.remove(primaryKey);
            }
        }
    }

    /**
     * Performs an exact-match query on secondary indexes.
     *
     * @param field         the field name to query
     * @param valueToSearch the exact value to match
     * @return set of matching primary keys
     */
    public Set<String> searchEquals(String field, String valueToSearch) {
        ConcurrentSkipListMap<String, Set<String>> indexMap = secondaryIndexes.get(field);
        if (indexMap == null || valueToSearch == null) {
            return Collections.emptySet();
        }
        Set<String> results = indexMap.get(valueToSearch);
        return results == null ? Collections.emptySet() : Collections.unmodifiableSet(results);
    }

    /**
     * Performs a range scan query on range indexes.
     *
     * @param field    the field name to query
     * @param startVal inclusive start boundary (null represents unbounded start)
     * @param endVal   inclusive end boundary (null represents unbounded end)
     * @return set of primary keys matching within the range boundaries
     */
    public Set<String> searchRange(String field, String startVal, String endVal) {
        ConcurrentSkipListMap<String, Set<String>> indexMap = secondaryIndexes.get(field);
        if (indexMap == null) {
            return Collections.emptySet();
        }

        SortedMap<String, Set<String>> rangeMap;
        if (startVal != null && endVal != null) {
            // Both bounds present. We append small padding char to endVal to include it (inclusive lookup)
            rangeMap = indexMap.subMap(startVal, endVal + "\u0000");
        } else if (startVal != null) {
            rangeMap = indexMap.tailMap(startVal);
        } else if (endVal != null) {
            rangeMap = indexMap.headMap(endVal + "\u0000");
        } else {
            rangeMap = indexMap;
        }

        Set<String> results = new HashSet<>();
        for (Set<String> keys : rangeMap.values()) {
            results.addAll(keys);
        }
        return results;
    }

    /**
     * Performs a prefix matching search.
     *
     * @param field  the field name to query
     * @param prefix the prefix pattern to match
     * @return set of primary keys matching prefix
     */
    public Set<String> searchPrefix(String field, String prefix) {
        PrefixIndexTrie trie = prefixIndexes.get(field);
        if (trie == null || prefix == null) {
            return Collections.emptySet();
        }
        return trie.searchPrefix(prefix);
    }

    private void ttlCleanerLoop() {
        while (running.get()) {
            try {
                Thread.sleep(500);
                long now = System.currentTimeMillis();

                // Find all expired items
                SortedMap<Long, Set<String>> expiredMap = ttlIndex.headMap(now + 1);
                if (expiredMap.isEmpty()) {
                    continue;
                }

                List<Long> expiredEpochs = new ArrayList<>(expiredMap.keySet());
                for (Long epoch : expiredEpochs) {
                    Set<String> keys = ttlIndex.remove(epoch);
                    if (keys != null) {
                        for (String key : keys) {
                            keyExpirations.remove(key);
                            logger.info("Evicting expired key '{}' via TTL Index.", key);
                            storageEngine.delete(key); // Evicts from storage and other indexes
                        }
                    }
                }
            } catch (InterruptedException ie) {
                break;
            } catch (Exception e) {
                logger.error("Exception in TTL cleanup task thread", e);
            }
        }
    }

    @Override
    public synchronized void close() {
        if (!running.getAndSet(false)) {
            return;
        }

        if (ttlCleanerThread != null) {
            ttlCleanerThread.interrupt();
            try {
                ttlCleanerThread.join(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        secondaryIndexes.clear();
        prefixIndexes.clear();
        ttlIndex.clear();
        keyExpirations.clear();
    }
}
