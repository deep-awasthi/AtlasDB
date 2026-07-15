package com.atlasdb.storage.index;

import java.util.*;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * A concurrent, thread-safe Trie implementation for Prefix Indexing.
 * Uses a volatile copy-on-write array for child nodes to achieve lock-free lookups.
 * Strictly avoids ConcurrentHashMap.
 */
public final class PrefixIndexTrie {

    private final TrieNode root = new TrieNode('\0');

    /**
     * Inserts a primary key associated with a field value string into the prefix index.
     *
     * @param value      the field value string (e.g., "John")
     * @param primaryKey the primary key of the record
     */
    public void insert(String value, String primaryKey) {
        if (value == null || primaryKey == null || value.isEmpty()) {
            return;
        }

        TrieNode current = root;
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            current = current.getOrCreateChild(c);
        }
        current.addPrimaryKey(primaryKey);
    }

    /**
     * Removes a primary key association from a field value string.
     *
     * @param value      the field value string
     * @param primaryKey the primary key of the record
     */
    public void remove(String value, String primaryKey) {
        if (value == null || primaryKey == null || value.isEmpty()) {
            return;
        }

        TrieNode current = root;
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            current = current.getChild(c);
            if (current == null) {
                return;
            }
        }
        current.removePrimaryKey(primaryKey);
    }

    /**
     * Searches for all primary keys whose associated field values start with the specified prefix.
     *
     * @param prefix the target search prefix
     * @return set of matching primary keys
     */
    public Set<String> searchPrefix(String prefix) {
        if (prefix == null) {
            return Collections.emptySet();
        }

        TrieNode current = root;
        for (int i = 0; i < prefix.length(); i++) {
            char c = prefix.charAt(i);
            current = current.getChild(c);
            if (current == null) {
                return Collections.emptySet();
            }
        }

        Set<String> results = new HashSet<>();
        collectDescendants(current, results);
        return results;
    }

    private void collectDescendants(TrieNode node, Set<String> results) {
        results.addAll(node.getPrimaryKeys());
        for (TrieNode child : node.getChildren()) {
            collectDescendants(child, results);
        }
    }

    /**
     * Node structure for the Prefix Trie.
     */
    private static final class TrieNode {
        private final char character;
        private volatile TrieNode[] children = new TrieNode[0];
        private final Set<String> primaryKeys = new CopyOnWriteArraySet<>();

        private TrieNode(char character) {
            this.character = character;
        }

        private Set<String> getPrimaryKeys() {
            return primaryKeys;
        }

        private void addPrimaryKey(String pk) {
            primaryKeys.add(pk);
        }

        private void removePrimaryKey(String pk) {
            primaryKeys.remove(pk);
        }

        private TrieNode[] getChildren() {
            return children;
        }

        /**
         * Returns child matching c, using binary search on the sorted children array (lock-free).
         */
        private TrieNode getChild(char c) {
            TrieNode[] snapshot = children;
            int low = 0;
            int high = snapshot.length - 1;

            while (low <= high) {
                int mid = (low + high) >>> 1;
                TrieNode midVal = snapshot[mid];
                int cmp = Character.compare(midVal.character, c);

                if (cmp < 0) {
                    low = mid + 1;
                } else if (cmp > 0) {
                    high = mid - 1;
                } else {
                    return midVal; // Key found
                }
            }
            return null; // Key not found
        }

        /**
         * Thread-safe copy-on-write insertion of a child node.
         */
        private synchronized TrieNode getOrCreateChild(char c) {
            TrieNode child = getChild(c);
            if (child != null) {
                return child;
            }

            TrieNode newChild = new TrieNode(c);
            TrieNode[] oldChildren = children;
            TrieNode[] newChildren = new TrieNode[oldChildren.length + 1];

            // Insert while maintaining sorted order
            int i = 0;
            while (i < oldChildren.length && oldChildren[i].character < c) {
                newChildren[i] = oldChildren[i];
                i++;
            }
            newChildren[i] = newChild;
            while (i < oldChildren.length) {
                newChildren[i + 1] = oldChildren[i];
                i++;
            }

            children = newChildren; // Volatile publish
            return newChild;
        }
    }
}
