package com.atlasdb.cluster.partition;

import com.atlasdb.cluster.ClusterNode;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Thread-safe Consistent Hash Ring implementation.
 * Projects physical nodes and virtual nodes (vnodes) onto a 64-bit circular hash space.
 */
public final class HashRing {

    private final TreeMap<Long, ClusterNode> ring = new TreeMap<>();
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final int vnodeCount;

    public HashRing(int vnodeCount) {
        if (vnodeCount <= 0) {
            throw new IllegalArgumentException("vnodeCount must be positive");
        }
        this.vnodeCount = vnodeCount;
    }

    /**
     * Registers a physical node on the ring by adding its virtual nodes.
     *
     * @param node the cluster node to add
     */
    public void addNode(ClusterNode node) {
        Objects.requireNonNull(node, "node cannot be null");
        lock.writeLock().lock();
        try {
            for (int i = 0; i < vnodeCount; i++) {
                String vnodeKey = node.getNodeId() + "-vnode-" + i;
                long hash = computeHash(vnodeKey);
                ring.put(hash, node);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Unregisters a physical node from the ring by removing all its virtual nodes.
     *
     * @param node the cluster node to remove
     */
    public void removeNode(ClusterNode node) {
        Objects.requireNonNull(node, "node cannot be null");
        lock.writeLock().lock();
        try {
            for (int i = 0; i < vnodeCount; i++) {
                String vnodeKey = node.getNodeId() + "-vnode-" + i;
                long hash = computeHash(vnodeKey);
                ring.remove(hash);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Resolves the primary owner node responsible for a given database key.
     *
     * @param key the database key
     * @return the cluster node owning the key, or null if the ring is empty
     */
    public ClusterNode getOwner(String key) {
        Objects.requireNonNull(key, "key cannot be null");
        lock.readLock().lock();
        try {
            if (ring.isEmpty()) {
                return null;
            }
            long hash = computeHash(key);
            Long ceilingKey = ring.ceilingKey(hash);
            if (ceilingKey == null) {
                ceilingKey = ring.firstKey();
            }
            return ring.get(ceilingKey);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Retrieves the current number of virtual nodes registered on the ring.
     */
    public int getRingSize() {
        lock.readLock().lock();
        try {
            return ring.size();
        } finally {
            lock.readLock().unlock();
        }
    }

    private long computeHash(String input) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest(input.getBytes(StandardCharsets.UTF_8));
            // Project first 8 bytes of digest to long integer
            long hash = 0;
            for (int i = 0; i < 8; i++) {
                hash = (hash << 8) | (digest[i] & 0xFF);
            }
            return hash;
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("Failed to initialize message digest", e);
        }
    }
}
