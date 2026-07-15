package com.atlasdb.replication;

import com.atlasdb.cluster.ClusterManager;
import com.atlasdb.cluster.ClusterNode;
import com.atlasdb.cluster.NodeState;
import com.atlasdb.cluster.config.ClusterConfig;
import com.atlasdb.cluster.partition.PartitionManager;
import com.atlasdb.network.client.TcpClient;
import com.atlasdb.network.protocol.Packet;
import com.atlasdb.network.server.TcpServer;
import com.atlasdb.replication.log.ReplicationEntry;
import com.atlasdb.replication.log.ReplicationLog;
import com.atlasdb.storage.StorageEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Coordinates quorum replication read and write operations.
 * Operates on top of PartitionManager and ClusterManager.
 */
public final class ReplicationManager {

    private static final Logger logger = LoggerFactory.getLogger(ReplicationManager.class);

    // Replication Custom Packet Types
    public static final byte TYPE_REPLICA_WRITE = 0x20;
    public static final byte TYPE_REPLICA_READ = 0x21;

    private final ClusterConfig config;
    private final StorageEngine<String, String> storageEngine;
    private final ClusterManager clusterManager;
    private final PartitionManager partitionManager;
    private final TcpServer tcpServer;

    private final ReplicationLog replicationLog = new ReplicationLog();
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

    // Health metrics
    private final Map<String, Integer> replicaFailureCounts = new HashMap<>();
    private final ReentrantLock healthLock = new ReentrantLock();

    public ReplicationManager(ClusterConfig config, StorageEngine<String, String> storageEngine,
                              ClusterManager clusterManager, PartitionManager partitionManager, TcpServer tcpServer) {
        this.config = Objects.requireNonNull(config, "config cannot be null");
        this.storageEngine = Objects.requireNonNull(storageEngine, "storageEngine cannot be null");
        this.clusterManager = Objects.requireNonNull(clusterManager, "clusterManager cannot be null");
        this.partitionManager = Objects.requireNonNull(partitionManager, "partitionManager cannot be null");
        this.tcpServer = Objects.requireNonNull(tcpServer, "tcpServer cannot be null");

        // Register custom packet handler hooks with TCP Server
        this.tcpServer.registerHandler(this::handleCustomPacket);
    }

    /**
     * Shuts down background replication tasks.
     */
    public void shutdown() {
        executor.shutdownNow();
    }

    /**
     * Executes a replicated write operation with the specified ConsistencyLevel.
     *
     * @param key   the key to insert/update
     * @param value the value
     * @param level the write consistency level
     * @return the monotonic version timestamp of the write
     */
    public long putReplicated(String key, String value, ConsistencyLevel level) {
        long version = System.currentTimeMillis();
        
        // 1. Get replica nodes group (Dynamo-style successor list)
        List<ClusterNode> group = getReplicationGroup(key);
        if (group.isEmpty()) {
            throw new IllegalStateException("Replication group is empty.");
        }

        // 2. Append locally first
        ReplicationEntry localEntry = replicationLog.append(key, value, version, false);
        storageEngine.put(key, value, version);

        int n = group.size();
        int w = getQuorumSize(level, n);
        
        // Local write counts as 1 success
        AtomicInteger successes = new AtomicInteger(1);
        CountDownLatch latch = new CountDownLatch(group.size() - 1);

        for (ClusterNode node : group) {
            if (node.getNodeId().equalsIgnoreCase(config.nodeId())) {
                continue;
            }
            executor.submit(() -> {
                try {
                    boolean ok = sendReplicaWrite(node, localEntry);
                    if (ok) {
                        successes.incrementAndGet();
                    } else {
                        recordReplicaFailure(node.getNodeId());
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        try {
            latch.await(2000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        if (successes.get() < w) {
            throw new RuntimeException("Replication Write Quorum failure. Required: " 
                    + w + ", Successes: " + successes.get());
        }

        return version;
    }

    /**
     * Executes a replicated delete operation with the specified ConsistencyLevel.
     */
    public boolean deleteReplicated(String key, ConsistencyLevel level) {
        long version = System.currentTimeMillis();
        List<ClusterNode> group = getReplicationGroup(key);
        if (group.isEmpty()) {
            return false;
        }

        ReplicationEntry localEntry = replicationLog.append(key, null, version, true);
        boolean deletedLocally = storageEngine.delete(key, version);

        int n = group.size();
        int w = getQuorumSize(level, n);
        AtomicInteger successes = new AtomicInteger(deletedLocally ? 1 : 0);
        CountDownLatch latch = new CountDownLatch(group.size() - 1);

        for (ClusterNode node : group) {
            if (node.getNodeId().equalsIgnoreCase(config.nodeId())) {
                continue;
            }
            executor.submit(() -> {
                try {
                    boolean ok = sendReplicaWrite(node, localEntry);
                    if (ok) {
                        successes.incrementAndGet();
                    } else {
                        recordReplicaFailure(node.getNodeId());
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        try {
            latch.await(2000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        if (successes.get() < w) {
            throw new RuntimeException("Replication Delete Quorum failure. Required: " 
                    + w + ", Successes: " + successes.get());
        }

        return deletedLocally;
    }

    /**
     * Executes a quorum read, comparing versions and triggering async read repairs.
     */
    public String getReplicated(String key, ConsistencyLevel level) {
        List<ClusterNode> group = getReplicationGroup(key);
        if (group.isEmpty()) {
            return null;
        }

        int n = group.size();
        int r = getQuorumSize(level, n);

        List<ReplicaReadResult> results = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch latch = new CountDownLatch(group.size());

        for (ClusterNode node : group) {
            if (node.getNodeId().equalsIgnoreCase(config.nodeId())) {
                executor.submit(() -> {
                    try {
                        String localVal = storageEngine.get(key);
                        long localVer = storageEngine.getVersion(key);
                        results.add(new ReplicaReadResult(config.nodeId(), localVer, localVal, localVer == -1 || localVal == null));
                    } finally {
                        latch.countDown();
                    }
                });
            } else {
                executor.submit(() -> {
                    try {
                        ReplicaReadResult res = sendReplicaRead(node, key);
                        if (res != null) {
                            results.add(res);
                        } else {
                            recordReplicaFailure(node.getNodeId());
                        }
                    } finally {
                        latch.countDown();
                    }
                });
            }
        }

        try {
            latch.await(2000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        if (results.size() < r) {
            throw new RuntimeException("Replication Read Quorum failure. Required: " 
                    + r + ", Responses: " + results.size());
        }

        // Version comparison
        ReplicaReadResult newest = null;
        for (ReplicaReadResult res : results) {
            if (newest == null || res.version > newest.version) {
                newest = res;
            }
        }

        if (newest == null || newest.version == -1 || newest.isDeleted) {
            return null;
        }

        // Trigger Read Repair asynchronously for any stale replicas
        for (ReplicaReadResult res : results) {
            if (res.version < newest.version) {
                logger.info("READ REPAIR - Triggering repair for stale Node '{}' on key '{}' (version {} vs {})", 
                        res.nodeId, key, res.version, newest.version);
                
                // Find node configuration details
                ClusterNode staleNode = null;
                for (ClusterNode cn : group) {
                    if (cn.getNodeId().equalsIgnoreCase(res.nodeId)) {
                        staleNode = cn;
                        break;
                    }
                }
                if (staleNode != null) {
                    ClusterNode targetNode = staleNode;
                    ReplicaReadResult repairSource = newest;
                    executor.submit(() -> {
                        ReplicationEntry repairEntry = new ReplicationEntry(0, key, repairSource.value, repairSource.version, false);
                        sendReplicaWrite(targetNode, repairEntry);
                    });
                }
            }
        }

        return newest.value;
    }

    /**
     * Resolves the replication nodes group (Dynamo-style successor list starting from partition owner).
     */
    public List<ClusterNode> getReplicationGroup(String key) {
        Map<String, ClusterNode> members = clusterManager.getMembership();
        List<ClusterNode> allNodes = members.values().stream()
                .sorted(Comparator.comparing(ClusterNode::getNodeId))
                .toList();

        List<ClusterNode> group = new ArrayList<>();
        if (allNodes.isEmpty()) {
            return group;
        }

        ClusterNode primary = partitionManager.getRoute(key);
        if (primary == null) {
            return group;
        }

        int primaryIdx = -1;
        for (int i = 0; i < allNodes.size(); i++) {
            if (allNodes.get(i).getNodeId().equalsIgnoreCase(primary.getNodeId())) {
                primaryIdx = i;
                break;
            }
        }

        if (primaryIdx == -1) {
            primaryIdx = 0;
        }

        int replicationFactor = Math.min(3, allNodes.size());
        for (int i = 0; i < replicationFactor; i++) {
            int idx = (primaryIdx + i) % allNodes.size();
            group.add(allNodes.get(idx));
        }

        return group;
    }

    private int getQuorumSize(ConsistencyLevel level, int replicationFactor) {
        return switch (level) {
            case ONE -> 1;
            case QUORUM -> (replicationFactor / 2) + 1;
            case ALL -> replicationFactor;
        };
    }

    private boolean sendReplicaWrite(ClusterNode target, ReplicationEntry entry) {
        // client port is clusterPort - 1000
        int clientPort = target.getPort() - 1000;
        try (TcpClient client = new TcpClient(target.getHost(), clientPort, 2000)) {
            client.connect();

            byte[] keyBytes = entry.key().getBytes(StandardCharsets.UTF_8);
            byte[] valBytes = entry.value() == null ? new byte[0] : entry.value().getBytes(StandardCharsets.UTF_8);

            ByteBuffer payload = ByteBuffer.allocate(8 + 8 + 1 + 4 + keyBytes.length + 4 + valBytes.length);
            payload.putLong(entry.lsn());
            payload.putLong(entry.timestamp());
            payload.put((byte) (entry.isDelete() ? 1 : 0));
            payload.putInt(keyBytes.length);
            payload.put(keyBytes);
            payload.putInt(valBytes.length);
            payload.put(valBytes);
            payload.flip();

            Packet response = client.send(new Packet(TYPE_REPLICA_WRITE, 1, 0, payload.array()));
            return response.getType() == Packet.TYPE_RESP_SUCCESS;
        } catch (Exception e) {
            logger.debug("Failed sending replica write to Node '{}': {}", target.getNodeId(), e.getMessage());
            return false;
        }
    }

    private ReplicaReadResult sendReplicaRead(ClusterNode target, String key) {
        int clientPort = target.getPort() - 1000;
        try (TcpClient client = new TcpClient(target.getHost(), clientPort, 2000)) {
            client.connect();

            byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
            Packet response = client.send(new Packet(TYPE_REPLICA_READ, 2, 0, keyBytes));

            if (response.getType() == Packet.TYPE_RESP_SUCCESS) {
                ByteBuffer buf = ByteBuffer.wrap(response.getPayload());
                long version = buf.getLong();
                boolean isDeleted = buf.get() == 1;
                int valLen = buf.getInt();
                byte[] valBytes = new byte[valLen];
                buf.get(valBytes);
                String value = valLen > 0 ? new String(valBytes, StandardCharsets.UTF_8) : null;

                return new ReplicaReadResult(target.getNodeId(), version, value, isDeleted);
            }
        } catch (Exception e) {
            logger.debug("Failed sending replica read to Node '{}': {}", target.getNodeId(), e.getMessage());
        }
        return null;
    }

    private void recordReplicaFailure(String nodeId) {
        healthLock.lock();
        try {
            int count = replicaFailureCounts.getOrDefault(nodeId, 0) + 1;
            replicaFailureCounts.put(nodeId, count);
            if (count > 5) {
                logger.warn("REPLICA HEALTH WARNING - Replica node '{}' has failed {} consecutive checks. Currently DEGRADED.", 
                        nodeId, count);
            }
        } finally {
            healthLock.unlock();
        }
    }

    private boolean handleCustomPacket(SocketChannel client, Packet packet) throws IOException {
        if (packet.getType() == TYPE_REPLICA_WRITE) {
            ByteBuffer buf = ByteBuffer.wrap(packet.getPayload());
            long lsn = buf.getLong();
            long timestamp = buf.getLong();
            boolean isDelete = buf.get() == 1;
            
            int keyLen = buf.getInt();
            byte[] keyBytes = new byte[keyLen];
            buf.get(keyBytes);
            String key = new String(keyBytes, StandardCharsets.UTF_8);

            int valLen = buf.getInt();
            String value = null;
            if (valLen > 0) {
                byte[] valBytes = new byte[valLen];
                buf.get(valBytes);
                value = new String(valBytes, StandardCharsets.UTF_8);
            }

            // Apply write locally on follower
            if (isDelete) {
                storageEngine.delete(key, timestamp);
            } else {
                storageEngine.put(key, value, timestamp);
            }

            replicationLog.appendEntry(new ReplicationEntry(lsn, key, value, timestamp, isDelete));

            // Respond success containing LSN
            byte[] respPayload = ByteBuffer.allocate(8).putLong(lsn).array();
            tcpServer.sendPacket(client, new Packet(Packet.TYPE_RESP_SUCCESS, packet.getRequestId(), 0, respPayload));
            return true;
        }

        if (packet.getType() == TYPE_REPLICA_READ) {
            String key = new String(packet.getPayload(), StandardCharsets.UTF_8);
            String val = storageEngine.get(key);
            long ver = storageEngine.getVersion(key);

            byte[] valBytes = val == null ? new byte[0] : val.getBytes(StandardCharsets.UTF_8);
            boolean isDeleted = (ver != -1 && val == null);

            ByteBuffer resp = ByteBuffer.allocate(8 + 1 + 4 + valBytes.length);
            resp.putLong(ver);
            resp.put((byte) (isDeleted ? 1 : 0));
            resp.putInt(valBytes.length);
            resp.put(valBytes);
            resp.flip();

            tcpServer.sendPacket(client, new Packet(Packet.TYPE_RESP_SUCCESS, packet.getRequestId(), 0, resp.array()));
            return true;
        }

        return false;
    }

    private static class ReplicaReadResult {
        final String nodeId;
        final long version;
        final String value;
        final boolean isDeleted;

        ReplicaReadResult(String nodeId, long version, String value, boolean isDeleted) {
            this.nodeId = nodeId;
            this.version = version;
            this.value = value;
            this.isDeleted = isDeleted;
        }
    }
}
