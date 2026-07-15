package com.atlasdb.storage.config;

/**
 * Immutable configuration class representing storage engine settings.
 *
 * @param initialCapacity the initial number of buckets in the hash table (must be power of two)
 * @param loadFactor      the load factor threshold at which resizing occurs (must be positive)
 */
public record StorageConfig(int initialCapacity, float loadFactor, String dbMode) {

    public StorageConfig(int initialCapacity, float loadFactor) {
        this(initialCapacity, loadFactor, "HYBRID");
    }

    /**
     * Constructs a StorageConfig and validates capacity and load factor constraints.
     */
    public StorageConfig {
        if (initialCapacity <= 0 || (initialCapacity & (initialCapacity - 1)) != 0) {
            throw new IllegalArgumentException("Initial capacity must be a power of two greater than zero.");
        }
        if (loadFactor <= 0.0f || Float.isNaN(loadFactor)) {
            throw new IllegalArgumentException("Load factor must be a positive number.");
        }
        if (dbMode == null) {
            dbMode = "HYBRID";
        }
        dbMode = dbMode.toUpperCase();
        if (!dbMode.equals("SQL") && !dbMode.equals("NOSQL") && !dbMode.equals("HYBRID")) {
            throw new IllegalArgumentException("dbMode must be 'SQL', 'NOSQL', or 'HYBRID'");
        }
    }

    /**
     * Creates a default configuration with 16 initial capacity and 0.75 load factor.
     *
     * @return the default storage configuration
     */
    public static StorageConfig defaultConfiguration() {
        return new StorageConfig(16, 0.75f, "HYBRID");
    }
}
