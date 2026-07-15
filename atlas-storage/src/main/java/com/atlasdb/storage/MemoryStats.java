package com.atlasdb.storage;

/**
 * Immutable record representing the memory consumption statistics of the storage engine.
 *
 * @param entryCount    the total number of key-value entries
 * @param bucketCount   the number of buckets in the active hash table
 * @param keyBytes      the estimated heap memory used by keys in bytes
 * @param valueBytes    the estimated heap memory used by values in bytes
 * @param overheadBytes the estimated database index overhead in bytes (buckets, entries, locks)
 * @param totalBytes    the total estimated heap memory consumption in bytes (keys + values + overhead)
 */
public record MemoryStats(
        long entryCount,
        long bucketCount,
        long keyBytes,
        long valueBytes,
        long overheadBytes,
        long totalBytes
) {
}
