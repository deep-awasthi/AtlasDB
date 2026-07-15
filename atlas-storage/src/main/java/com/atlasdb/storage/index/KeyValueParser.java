package com.atlasdb.storage.index;

/**
 * Utility parser to extract field values from comma-separated Key-Value record strings
 * (e.g., extracting "30" from "age=30,city=Boston" for the field "age").
 * Built from scratch using index lookup to optimize parsing performance.
 */
public final class KeyValueParser {

    private KeyValueParser() {
        // Prevent instantiation
    }

    /**
     * Extracts the value of a specific field from a key-value record string.
     *
     * @param record    the database record string (e.g., "age=30,city=Boston")
     * @param fieldName the name of the field to extract (e.g., "age")
     * @return the extracted value string, or null if the field is not found
     */
    public static String parseFieldValue(String record, String fieldName) {
        if (record == null || fieldName == null || record.isEmpty() || fieldName.isEmpty()) {
            return null;
        }

        int targetKeyLen = fieldName.length();
        int recordLen = record.length();
        int start = 0;

        while (start < recordLen) {
            // Find key match at start
            if (record.startsWith(fieldName, start)) {
                int eqIdx = start + targetKeyLen;
                if (eqIdx < recordLen && record.charAt(eqIdx) == '=') {
                    // Match found, now find the end of the value (either a comma or end of string)
                    int valStart = eqIdx + 1;
                    int commaIdx = record.indexOf(',', valStart);
                    if (commaIdx == -1) {
                        return record.substring(valStart).trim();
                    } else {
                        return record.substring(valStart, commaIdx).trim();
                    }
                }
            }

            // Move start to the next key-value pair after the next comma
            int nextComma = record.indexOf(',', start);
            if (nextComma == -1) {
                break;
            }
            start = nextComma + 1;
            // Trim leading spaces if any after comma
            while (start < recordLen && Character.isWhitespace(record.charAt(start))) {
                start++;
            }
        }

        return null;
    }
}
