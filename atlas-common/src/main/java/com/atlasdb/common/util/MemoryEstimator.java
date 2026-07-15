package com.atlasdb.common.util;

/**
 * Utility for estimating memory consumption of common Java objects in heap.
 * Assumes a 64-bit JVM with Compressed OOPs (Ordinary Object Pointers) enabled (default for heap < 32GB).
 * Alignment is assumed to be 8 bytes.
 */
public final class MemoryEstimator {

    private MemoryEstimator() {
        // Prevent instantiation
    }

    /**
     * Estimates the memory footprint of a given object.
     *
     * @param obj the object to estimate
     * @return the estimated size in bytes
     */
    public static long estimate(Object obj) {
        if (obj == null) {
            return 0;
        }

        if (obj instanceof String str) {
            return estimateString(str);
        } else if (obj instanceof byte[] bytes) {
            return estimateByteArray(bytes);
        } else if (obj instanceof Integer) {
            return 16; // Object header (12) + int (4) = 16 (already aligned)
        } else if (obj instanceof Long) {
            return 24; // Object header (12) + long (8) = 20 -> aligned to 24
        } else if (obj instanceof Double) {
            return 24; // Object header (12) + double (8) = 20 -> aligned to 24
        } else if (obj instanceof Float) {
            return 16; // Object header (12) + float (4) = 16 (already aligned)
        } else if (obj instanceof Short) {
            return 16; // Object header (12) + short (2) = 14 -> aligned to 16
        } else if (obj instanceof Byte) {
            return 16; // Object header (12) + byte (1) = 13 -> aligned to 16
        } else if (obj instanceof Character) {
            return 16; // Object header (12) + char (2) = 14 -> aligned to 16
        } else if (obj instanceof Boolean) {
            return 16; // Object header (12) + boolean (1) = 13 -> aligned to 16
        }

        // Fallback for custom or unknown types: assume basic object header + reference
        return 24;
    }

    /**
     * Estimates size of a String object on heap.
     * In modern Java (9+), String has:
     * - Object header: 12 bytes
     * - byte[] value: 4 bytes (reference)
     * - int hash: 4 bytes
     * - byte coder: 1 byte
     * - byte hashIsZero: 1 byte
     * Total fields + header = 22 bytes. Aligned to 8 bytes = 24 bytes.
     * Plus the byte[] value array:
     * - Array header: 16 bytes
     * - Length: N bytes
     * Total = 24 + align(16 + N).
     */
    private static long estimateString(String str) {
        int length = str.length();
        // Assuming LATIN1 (1 byte per char) or UTF16 (2 bytes per char) based on internal representation
        // We'll estimate worst case or average. Let's do length * 2 for conservative estimation.
        long arraySize = align(16L + ((long) length * 2));
        return 24 + arraySize;
    }

    private static long estimateByteArray(byte[] bytes) {
        return align(16L + bytes.length);
    }

    /**
     * Aligns the size to the next multiple of 8 bytes.
     *
     * @param size the size to align
     * @return the aligned size
     */
    public static long align(long size) {
        return (size + 7) & ~7;
    }
}
