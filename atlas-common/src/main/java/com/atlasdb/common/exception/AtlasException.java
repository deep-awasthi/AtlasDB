package com.atlasdb.common.exception;

/**
 * Base exception class for all AtlasDB exceptions.
 * All custom exceptions in the database should extend this class.
 */
public class AtlasException extends RuntimeException {

    /**
     * Constructs a new AtlasException with the specified detail message.
     *
     * @param message the detail message
     */
    public AtlasException(String message) {
        super(message);
    }

    /**
     * Constructs a new AtlasException with the specified detail message and cause.
     *
     * @param message the detail message
     * @param cause   the cause
     */
    public AtlasException(String message, Throwable cause) {
        super(message, cause);
    }
}
