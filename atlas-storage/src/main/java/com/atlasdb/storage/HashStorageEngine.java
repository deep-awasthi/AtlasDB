package com.atlasdb.storage;

import com.atlasdb.common.VersionGenerator;
import com.atlasdb.common.exception.StorageException;
import com.atlasdb.common.util.MemoryEstimator;
import com.atlasdb.storage.config.StorageConfig;
import com.atlasdb.storage.persistence.WalEntry;
import com.atlasdb.storage.persistence.WriteAheadLog;
import com.atlasdb.storage.index.IndexManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.StampedLock;

/**
 * Production-quality, custom concurrent hash table implementation of StorageEngine.
 * Refactored to support Multi-Version Concurrency Control (MVCC) with lock-free snapshot reads,
 * version chains, and a background garbage collector.
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public final class HashStorageEngine<K, V> implements StorageEngine<K, V> {

    private static final Logger logger = LoggerFactory.getLogger(HashStorageEngine.class);

    private final StorageConfig config;
    private final VersionGenerator versionGenerator;
    
    // Global lock for resizing operations.
    // Regular CRUD operations acquire a read lock, resizing acquires a write lock.
    private final StampedLock resizeLock = new StampedLock();
    
    private volatile Bucket<K, V>[] buckets;
    private volatile WriteAheadLog wal;
    private volatile IndexManager indexManager;
    
    private final AtomicInteger size = new AtomicInteger(0);
    private final AtomicLong keyBytes = new AtomicLong(0);
    private final AtomicLong valueBytes = new AtomicLong(0);
    private final AtomicLong overheadBytes = new AtomicLong(0);

    /**
     * Represents the result of a version chain pruning operation.
     *
     * @param prunedVersions the number of obsolete versions pruned
     * @param fullyDeleted   true if the key was fully evicted as a tombstone
     */
    public record PruneResult(int prunedVersions, boolean fullyDeleted) {}

    /**
     * Registers the IndexManager to coordinate index updates on write/delete mutations.
     *
     * @param indexManager the active index manager instance
     */
    public void registerIndexManager(IndexManager indexManager) {
        this.indexManager = indexManager;
    }

    /**
     * Registers the Write-Ahead Log to compile log frames on every mutation.
     *
     * @param wal the active write-ahead log instance
     */
    public void registerWal(WriteAheadLog wal) {
        this.wal = wal;
    }

    public String getDbMode() {
        return config.dbMode();
    }

    /**
     * Constructs a HashStorageEngine with the specified configuration and version generator.
     */
    @SuppressWarnings("unchecked")
    public HashStorageEngine(StorageConfig config, VersionGenerator versionGenerator) {
        if (config == null) {
            throw new IllegalArgumentException("Configuration cannot be null.");
        }
        if (versionGenerator == null) {
            throw new IllegalArgumentException("Version generator cannot be null.");
        }
        this.config = config;
        this.versionGenerator = versionGenerator;
        
        int capacity = config.initialCapacity();
        this.buckets = (Bucket<K, V>[]) new Bucket[capacity];
        for (int i = 0; i < capacity; i++) {
            this.buckets[i] = new Bucket<>();
        }
        
        // Calculate initial overhead
        long initialOverhead = calculateTableOverhead(capacity);
        this.overheadBytes.set(initialOverhead);
    }

    private int hash(Object key) {
        if (key == null) {
            return 0;
        }
        int h = key.hashCode();
        return h ^ (h >>> 16);
    }

    private int getIndex(int hash, int length) {
        return hash & (length - 1);
    }

    @Override
    public long put(K key, V value) {
        return put(key, value, 0);
    }

    @Override
    public long put(K key, V value, long version) {
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null.");
        }
        if (value == null) {
            throw new IllegalArgumentException("Value cannot be null.");
        }

        long resizeStamp = resizeLock.readLock();
        try {
            WriteAheadLog activeWal = this.wal;
            long newVersion = version;
            if (activeWal != null) {
                if (!(key instanceof String) || !(value instanceof String)) {
                    throw new StorageException("WAL only supports String keys and values.");
                }
                if (newVersion == 0) {
                    newVersion = versionGenerator.nextVersion();
                }
                versionGenerator.advanceTo(newVersion);
                try {
                    activeWal.append(new WalEntry(WalEntry.TYPE_PUT, newVersion, (String) key, (String) value));
                } catch (IOException e) {
                    throw new StorageException("Write-Ahead Logging failed: " + e.getMessage(), e);
                }
            }

            int h = hash(key);
            Bucket<K, V>[] currentBuckets = this.buckets;
            int idx = getIndex(h, currentBuckets.length);
            Bucket<K, V> bucket = currentBuckets[idx];

            String oldValStr = null;

            long bucketStamp = bucket.writeLock();
            try {
                if (newVersion == 0) {
                    newVersion = versionGenerator.nextVersion();
                }
                versionGenerator.advanceTo(newVersion);
                Entry<K, V> prev = null;
                Entry<K, V> curr = bucket.getHead();
                while (curr != null) {
                    if (curr.getKey().equals(key)) {
                        // Key exists: prepend new VersionNode to version chain
                        VersionNode<V> oldHead = curr.getVersionHead();
                        oldValStr = (oldHead != null && !oldHead.isDeleted()) ? (String) oldHead.getValue() : null;

                        VersionNode<V> newHead = new VersionNode<>(value, newVersion, false, oldHead);
                        curr.setVersionHead(newHead);

                        // If it was previously tombstoned/deleted, increment active size
                        if (oldValStr == null) {
                            size.incrementAndGet();
                        }

                        // Update memory stats
                        long newValSize = MemoryEstimator.estimate(value);
                        valueBytes.addAndGet(newValSize);
                        overheadBytes.addAndGet(32); // VersionNode overhead

                        bucket.unlockWrite(bucketStamp);
                        bucketStamp = 0;
                        break;
                    }
                    prev = curr;
                    curr = curr.getNext();
                }

                if (bucketStamp != 0) {
                    // Key does not exist: insert new Entry with new VersionNode
                    VersionNode<V> versionNode = new VersionNode<>(value, newVersion, false, null);
                    Entry<K, V> newEntry = new Entry<>(key, versionNode, null);
                    if (prev == null) {
                        bucket.setHead(newEntry);
                    } else {
                        prev.setNext(newEntry);
                    }

                    // Update memory stats
                    long keySize = MemoryEstimator.estimate(key);
                    long valSize = MemoryEstimator.estimate(value);
                    keyBytes.addAndGet(keySize);
                    valueBytes.addAndGet(valSize);
                    overheadBytes.addAndGet(32 + 32); // Entry (32) + VersionNode (32) overhead

                    size.incrementAndGet();
                    
                    // Check if we need to resize
                    if (size.get() > currentBuckets.length * config.loadFactor()) {
                        bucket.unlockWrite(bucketStamp);
                        bucketStamp = 0; // mark as unlocked
                        resizeLock.unlockRead(resizeStamp);
                        resizeStamp = 0; // mark as unlocked
                        
                        checkAndResize(currentBuckets.length);
                    }
                }
                
                // Notify index manager
                IndexManager im = this.indexManager;
                if (im != null) {
                    im.onPut((String) key, oldValStr, (String) value);
                }

                return newVersion;
            } finally {
                if (bucketStamp != 0) {
                    bucket.unlockWrite(bucketStamp);
                }
            }
        } finally {
            if (resizeStamp != 0) {
                resizeLock.unlockRead(resizeStamp);
            }
        }
    }

    @Override
    public V get(K key) {
        // Read Committed: gets latest committed timestamp
        return get(key, versionGenerator.currentVersion());
    }

    /**
     * Performs a Snapshot Read of a key at the specified read timestamp.
     * Lock-free read traversal of the version chain.
     *
     * @param key           the target key
     * @param readTimestamp the read snapshot timestamp
     * @return the value visible at the timestamp, or null if deleted or not found
     */
    public V get(K key, long readTimestamp) {
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null.");
        }

        long resizeStamp = resizeLock.readLock();
        try {
            int h = hash(key);
            Bucket<K, V>[] currentBuckets = this.buckets;
            int idx = getIndex(h, currentBuckets.length);
            Bucket<K, V> bucket = currentBuckets[idx];

            // Try optimistic read first
            long optStamp = bucket.tryOptimisticRead();
            Entry<K, V> curr = bucket.getHead();
            V val = null;

            if (bucket.validate(optStamp)) {
                while (curr != null) {
                    if (curr.getKey().equals(key)) {
                        VersionNode<V> versionNode = curr.getVersionHead();
                        while (versionNode != null) {
                            if (versionNode.getTimestamp() <= readTimestamp) {
                                val = versionNode.isDeleted() ? null : versionNode.getValue();
                                break;
                            }
                            versionNode = versionNode.getNext();
                        }
                        break;
                    }
                    curr = curr.getNext();
                }
                if (bucket.validate(optStamp)) {
                    return val;
                }
            }

            // Fallback to pessimistic read lock
            long readStamp = bucket.readLock();
            try {
                curr = bucket.getHead();
                while (curr != null) {
                    if (curr.getKey().equals(key)) {
                        VersionNode<V> versionNode = curr.getVersionHead();
                        while (versionNode != null) {
                            if (versionNode.getTimestamp() <= readTimestamp) {
                                return versionNode.isDeleted() ? null : versionNode.getValue();
                            }
                            versionNode = versionNode.getNext();
                        }
                        return null;
                    }
                    curr = curr.getNext();
                }
                return null;
            } finally {
                bucket.unlockRead(readStamp);
            }
        } finally {
            resizeLock.unlockRead(resizeStamp);
        }
    }

    @Override
    public long getVersion(K key) {
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null.");
        }

        long resizeStamp = resizeLock.readLock();
        try {
            int h = hash(key);
            Bucket<K, V>[] currentBuckets = this.buckets;
            int idx = getIndex(h, currentBuckets.length);
            Bucket<K, V> bucket = currentBuckets[idx];

            long readStamp = bucket.readLock();
            try {
                Entry<K, V> curr = bucket.getHead();
                while (curr != null) {
                    if (curr.getKey().equals(key)) {
                        VersionNode<V> head = curr.getVersionHead();
                        return head != null ? head.getTimestamp() : -1L;
                    }
                    curr = curr.getNext();
                }
                return -1L;
            } finally {
                bucket.unlockRead(readStamp);
            }
        } finally {
            resizeLock.unlockRead(resizeStamp);
        }
    }

    @Override
    public boolean delete(K key) {
        return delete(key, 0);
    }

    @Override
    public boolean delete(K key, long version) {
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null.");
        }

        long resizeStamp = resizeLock.readLock();
        try {
            WriteAheadLog activeWal = this.wal;
            long deleteVersion = version;
            if (activeWal != null) {
                if (!(key instanceof String)) {
                    throw new StorageException("WAL only supports String keys.");
                }
                if (deleteVersion == 0) {
                    deleteVersion = versionGenerator.nextVersion();
                }
                versionGenerator.advanceTo(deleteVersion);
                try {
                    activeWal.append(new WalEntry(WalEntry.TYPE_DELETE, deleteVersion, (String) key, null));
                } catch (IOException e) {
                    throw new StorageException("Write-Ahead Logging failed: " + e.getMessage(), e);
                }
            }

            int h = hash(key);
            Bucket<K, V>[] currentBuckets = this.buckets;
            int idx = getIndex(h, currentBuckets.length);
            Bucket<K, V> bucket = currentBuckets[idx];

            String oldValStr = null;
            boolean deleted = false;

            long writeStamp = bucket.writeLock();
            try {
                if (deleteVersion == 0) {
                    deleteVersion = versionGenerator.nextVersion();
                }
                versionGenerator.advanceTo(deleteVersion);
                Entry<K, V> prev = null;
                Entry<K, V> curr = bucket.getHead();
                while (curr != null) {
                    if (curr.getKey().equals(key)) {
                        VersionNode<V> oldHead = curr.getVersionHead();
                        if (oldHead == null || oldHead.isDeleted()) {
                            // Already deleted (or empty)
                            break;
                        }

                        oldValStr = (String) oldHead.getValue();
                        deleted = true;

                        // Prepend a tombstone node
                        VersionNode<V> tombstone = new VersionNode<>(null, deleteVersion, true, oldHead);
                        curr.setVersionHead(tombstone);

                        // Decrement active size
                        size.decrementAndGet();

                        // Update statistics: we add 32 bytes for the new tombstone node
                        overheadBytes.addAndGet(32);
                        break;
                    }
                    prev = curr;
                    curr = curr.getNext();
                }
                
                bucket.unlockWrite(writeStamp);
                writeStamp = 0;

                if (deleted) {
                    IndexManager im = this.indexManager;
                    if (im != null) {
                        im.onDelete((String) key, oldValStr);
                    }
                }

                return deleted;
            } finally {
                if (writeStamp != 0) {
                    bucket.unlockWrite(writeStamp);
                }
            }
        } finally {
            resizeLock.unlockRead(resizeStamp);
        }
    }

    /**
     * Prunes obsolete versions of a key's version chain that are older than minActiveTimestamp.
     * If the visible version is a tombstone, and no active reader can see previous versions,
     * the entire key/entry is removed from the storage engine.
     *
     * @param key                the target key
     * @param minActiveTimestamp the minimum active reader timestamp
     * @return a PruneResult outlining pruned and eviction state
     */
    public PruneResult pruneKey(K key, long minActiveTimestamp) {
        if (key == null) {
            return new PruneResult(0, false);
        }

        long resizeStamp = resizeLock.readLock();
        try {
            int h = hash(key);
            Bucket<K, V>[] currentBuckets = this.buckets;
            int idx = getIndex(h, currentBuckets.length);
            Bucket<K, V> bucket = currentBuckets[idx];

            long writeStamp = bucket.writeLock();
            try {
                Entry<K, V> prev = null;
                Entry<K, V> curr = bucket.getHead();
                while (curr != null) {
                    if (curr.getKey().equals(key)) {
                        VersionNode<V> prevNode = null;
                        VersionNode<V> node = curr.getVersionHead();

                        // Locate the first version visible at or before minActiveTimestamp
                        while (node != null && node.getTimestamp() > minActiveTimestamp) {
                            prevNode = node;
                            node = node.getNext();
                        }

                        // node is the first node visible at/before minActiveTimestamp.
                        // Any node following it (older versions) is safely garbage collectable.
                        int prunedCount = 0;
                        if (node != null && node.getNext() != null) {
                            VersionNode<V> toPrune = node.getNext();
                            node.setNext(null); // Cut the chain

                            long freedValBytes = 0;
                            long freedOverheadBytes = 0;
                            while (toPrune != null) {
                                freedValBytes += MemoryEstimator.estimate(toPrune.getValue());
                                freedOverheadBytes += 32; // VersionNode overhead
                                prunedCount++;
                                toPrune = toPrune.getNext();
                            }
                            valueBytes.addAndGet(-freedValBytes);
                            overheadBytes.addAndGet(-freedOverheadBytes);
                        }

                        // Check if the baseline version is a tombstone and is currently the active head.
                        // If it is, and no readers can see older records, evict the entry completely.
                        boolean fullyDeleted = false;
                        VersionNode<V> head = curr.getVersionHead();
                        if (head != null && head.isDeleted() && head.getTimestamp() <= minActiveTimestamp) {
                            // Unlink entry
                            if (prev == null) {
                                bucket.setHead(curr.getNext());
                            } else {
                                prev.setNext(curr.getNext());
                            }

                            // Free remaining memory stats
                            long freedKeyBytes = MemoryEstimator.estimate(curr.getKey());
                            keyBytes.addAndGet(-freedKeyBytes);
                            overheadBytes.addAndGet(-32); // Entry object overhead

                            // Free any remaining version node overhead in memory stats
                            long versionNodeOverhead = 0;
                            VersionNode<V> temp = head;
                            while (temp != null) {
                                versionNodeOverhead += 32;
                                temp = temp.getNext();
                            }
                            overheadBytes.addAndGet(-versionNodeOverhead);

                            fullyDeleted = true;
                        }

                        return new PruneResult(prunedCount, fullyDeleted);
                    }
                    prev = curr;
                    curr = curr.getNext();
                }
                return new PruneResult(0, false);
            } finally {
                bucket.unlockWrite(writeStamp);
            }
        } finally {
            resizeLock.unlockRead(resizeStamp);
        }
    }

    @Override
    public boolean containsKey(K key) {
        return get(key) != null;
    }

    @Override
    public int size() {
        return size.get();
    }

    @Override
    @SuppressWarnings("unchecked")
    public void clear() {
        long writeStamp = resizeLock.writeLock();
        try {
            int capacity = config.initialCapacity();
            this.buckets = (Bucket<K, V>[]) new Bucket[capacity];
            for (int i = 0; i < capacity; i++) {
                this.buckets[i] = new Bucket<>();
            }
            size.set(0);
            keyBytes.set(0);
            valueBytes.set(0);
            overheadBytes.set(calculateTableOverhead(capacity));
        } finally {
            resizeLock.unlockWrite(writeStamp);
        }
    }

    @Override
    public MemoryStats getMemoryStats() {
        long keys = keyBytes.get();
        long values = valueBytes.get();
        long overhead = overheadBytes.get();
        return new MemoryStats(
                size.get(),
                buckets.length,
                keys,
                values,
                overhead,
                keys + values + overhead
        );
    }

    @Override
    public Iterator<Entry<K, V>> iterator() {
        return new WeaklyConsistentIterator();
    }

    @SuppressWarnings("unchecked")
    private void checkAndResize(int expectedCapacity) {
        long writeStamp = resizeLock.writeLock();
        try {
            // Double-checked capacity check
            if (buckets.length != expectedCapacity) {
                return;
            }

            int newCapacity = buckets.length * 2;
            logger.info("Resizing storage engine from {} to {} buckets", buckets.length, newCapacity);

            Bucket<K, V>[] newBuckets = (Bucket<K, V>[]) new Bucket[newCapacity];
            for (int i = 0; i < newCapacity; i++) {
                newBuckets[i] = new Bucket<>();
            }

            // Move elements to new table.
            // Since we hold the exclusive global write lock, no other thread is reading or writing
            // to the old buckets. We can copy them without individual bucket locks.
            for (Bucket<K, V> oldBucket : buckets) {
                Entry<K, V> curr = oldBucket.getHead();
                while (curr != null) {
                    Entry<K, V> next = curr.getNext();
                    int h = hash(curr.getKey());
                    int idx = getIndex(h, newCapacity);
                    
                    // Prepend/insert to new bucket chain
                    curr.setNext(newBuckets[idx].getHead());
                    newBuckets[idx].setHead(curr);

                    curr = next;
                }
            }

            this.buckets = newBuckets;
            // Calculate base array/bucket overhead + entry + VersionNode overheads currently present
            long tableOverhead = calculateTableOverhead(newCapacity);
            long itemsOverhead = 0;
            for (Bucket<K, V> b : newBuckets) {
                Entry<K, V> e = b.getHead();
                while (e != null) {
                    itemsOverhead += 32; // Entry
                    VersionNode<V> vn = e.getVersionHead();
                    while (vn != null) {
                        itemsOverhead += 32; // VersionNode
                        vn = vn.getNext();
                    }
                    e = e.getNext();
                }
            }
            this.overheadBytes.set(tableOverhead + itemsOverhead);
        } finally {
            resizeLock.unlockWrite(writeStamp);
        }
    }

    private long calculateTableOverhead(int capacity) {
        // bucket array header (16 bytes) + capacity * reference (4 bytes)
        long arrayBytes = MemoryEstimator.align(16L + ((long) capacity * 4));
        // Each bucket object (24 bytes) + its StampedLock (88 bytes)
        long bucketObjectsBytes = (long) capacity * (24 + 88);
        return arrayBytes + bucketObjectsBytes;
    }

    /**
     * Loads a key-value entry directly into the table with a specific version.
     * Bypasses the version generator and WAL log. Used for recovery bootstrapping.
     *
     * @param key     the key
     * @param value   the value
     * @param version the original version number
     */
    public void loadEntry(K key, V value, long version) {
        if (key == null || value == null) {
            throw new IllegalArgumentException("Key and Value cannot be null.");
        }

        // Advance version generator to match loaded version
        versionGenerator.advanceTo(version);

        long resizeStamp = resizeLock.readLock();
        try {
            int h = hash(key);
            Bucket<K, V>[] currentBuckets = this.buckets;
            int idx = getIndex(h, currentBuckets.length);
            Bucket<K, V> bucket = currentBuckets[idx];

            String oldValStr = null;

            long bucketStamp = bucket.writeLock();
            try {
                Entry<K, V> prev = null;
                Entry<K, V> curr = bucket.getHead();
                while (curr != null) {
                    if (curr.getKey().equals(key)) {
                        VersionNode<V> oldHead = curr.getVersionHead();
                        oldValStr = (oldHead != null && !oldHead.isDeleted()) ? (String) oldHead.getValue() : null;

                        // Overwrite/reset the version chain for snapshot restores
                        VersionNode<V> newHead = new VersionNode<>(value, version, false, null);
                        curr.setVersionHead(newHead);

                        if (oldValStr == null) {
                            size.incrementAndGet();
                        }

                        // Clear prior version list sizes and add new one
                        long oldValSize = (oldHead != null) ? MemoryEstimator.estimate(oldHead.getValue()) : 0;
                        long newValSize = MemoryEstimator.estimate(value);
                        valueBytes.addAndGet(newValSize - oldValSize);
                        
                        bucket.unlockWrite(bucketStamp);
                        bucketStamp = 0;
                        break;
                    }
                    prev = curr;
                    curr = curr.getNext();
                }

                if (bucketStamp != 0) {
                    VersionNode<V> versionNode = new VersionNode<>(value, version, false, null);
                    Entry<K, V> newEntry = new Entry<>(key, versionNode, null);
                    if (prev == null) {
                        bucket.setHead(newEntry);
                    } else {
                        prev.setNext(newEntry);
                    }

                    long keySize = MemoryEstimator.estimate(key);
                    long valSize = MemoryEstimator.estimate(value);
                    keyBytes.addAndGet(keySize);
                    valueBytes.addAndGet(valSize);
                    overheadBytes.addAndGet(32 + 32);
                    size.incrementAndGet();

                    if (size.get() > currentBuckets.length * config.loadFactor()) {
                        bucket.unlockWrite(bucketStamp);
                        bucketStamp = 0;
                        resizeLock.unlockRead(resizeStamp);
                        resizeStamp = 0;
                        checkAndResize(currentBuckets.length);
                    }
                }

                IndexManager im = this.indexManager;
                if (im != null) {
                    im.onPut((String) key, oldValStr, (String) value);
                }
            } finally {
                if (bucketStamp != 0) {
                    bucket.unlockWrite(bucketStamp);
                }
            }
        } finally {
            if (resizeStamp != 0) {
                resizeLock.unlockRead(resizeStamp);
            }
        }
    }

    /**
     * Weakly consistent iterator implementation.
     * Iterates over a snapshot of the buckets array.
     */
    private final class WeaklyConsistentIterator implements Iterator<Entry<K, V>> {
        private final Bucket<K, V>[] snapshotBuckets;
        private int bucketIndex = 0;
        private Entry<K, V> currentEntry = null;
        private Entry<K, V> nextEntry = null;

        private WeaklyConsistentIterator() {
            // Read lock to safely grab the buckets array snapshot reference
            long stamp = resizeLock.readLock();
            try {
                this.snapshotBuckets = buckets;
            } finally {
                resizeLock.unlockRead(stamp);
            }
            advance();
        }

        private void advance() {
            if (nextEntry != null) {
                nextEntry = nextEntry.getNext();
            }

            while (nextEntry == null && bucketIndex < snapshotBuckets.length) {
                Bucket<K, V> bucket = snapshotBuckets[bucketIndex++];
                // We use optimistic read on the bucket to fetch its head safely.
                // Since this is weakly consistent, we don't hold heavy locks.
                long stamp = bucket.tryOptimisticRead();
                Entry<K, V> head = bucket.getHead();
                if (!bucket.validate(stamp)) {
                    // Fallback to read lock if invalidated
                    long readStamp = bucket.readLock();
                    try {
                        head = bucket.getHead();
                    } finally {
                        bucket.unlockRead(readStamp);
                    }
                }
                nextEntry = head;
            }
        }

        @Override
        public boolean hasNext() {
            return nextEntry != null;
        }

        @Override
        public Entry<K, V> next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            currentEntry = nextEntry;
            advance();
            return currentEntry;
        }
    }
}
