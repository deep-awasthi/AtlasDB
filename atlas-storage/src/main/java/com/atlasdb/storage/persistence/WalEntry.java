package com.atlasdb.storage.persistence;

/**
 * Represents a single transaction record in the Write-Ahead Log (WAL).
 */
public final class WalEntry {

    public static final byte TYPE_PUT = 0x01;
    public static final byte TYPE_DELETE = 0x02;

    private final byte type;
    private final long version;
    private final String key;
    private final String value;

    /**
     * Constructs a WalEntry.
     *
     * @param type    the operation type (PUT or DELETE)
     * @param version the monotonic database version/timestamp
     * @param key     the target entry key
     * @param value   the target entry value (null for DELETE)
     */
    public WalEntry(byte type, long version, String key, String value) {
        if (type != TYPE_PUT && type != TYPE_DELETE) {
            throw new IllegalArgumentException("Invalid WAL entry type: " + type);
        }
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("Key cannot be null or empty in WAL entry.");
        }
        if (type == TYPE_PUT && value == null) {
            throw new IllegalArgumentException("Value cannot be null for PUT WAL entry.");
        }
        this.type = type;
        this.version = version;
        this.key = key;
        this.value = value;
    }

    /**
     * Retrieves the operation type.
     *
     * @return the type byte (TYPE_PUT or TYPE_DELETE)
     */
    public byte getType() {
        return type;
    }

    /**
     * Retrieves the database version of this log entry.
     *
     * @return the 64-bit version
     */
    public long getVersion() {
        return version;
    }

    /**
     * Retrieves the key.
     *
     * @return the entry key
     */
    public String getKey() {
        return key;
    }

    /**
     * Retrieves the value.
     *
     * @return the entry value, or null for DELETE operations
     */
    public String getValue() {
        return value;
    }
}
