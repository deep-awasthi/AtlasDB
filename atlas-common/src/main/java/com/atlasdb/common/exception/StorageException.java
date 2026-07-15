package com.atlasdb.common.exception;

/**
 * Exception thrown when a storage-related error occurs in AtlasDB.
 */
public class StorageException extends AtlasException {

    /**
     * Constructs a new StorageException with the specified detail message.
     *
     * @param message the detail message
     */
    public StorageException(String message) {
        super(message);
    }

    /**
     * Constructs a new StorageException with the specified detail message and cause.
     *
     * @param message the detail message
     * @param cause   the cause
     */
    public StorageException(String message, Throwable cause) {
        super(message, cause);
    }
}
