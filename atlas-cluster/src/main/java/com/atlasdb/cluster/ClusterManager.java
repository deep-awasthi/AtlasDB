package com.atlasdb.cluster;

import com.atlasdb.cluster.config.ClusterConfig;
import com.atlasdb.cluster.detect.FailureDetector;
import com.atlasdb.cluster.lead.LeaderSelector;
import com.atlasdb.cluster.protocol.ClusterMessage;
import com.atlasdb.network.client.TcpClient;
import com.atlasdb.network.protocol.Packet;
import com.atlasdb.network.protocol.PacketCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Orchestrates clustering logic: starts cluster sockets, gossips peer status,
 * executes heartbeats, runs failure sweep sweeps, and updates coordinator selections.
 */
public final class ClusterManager {

    private static final Logger logger = LoggerFactory.getLogger(ClusterManager.class);

    private final ClusterConfig config;
    private final Map<String, ClusterNode> membership = new HashMap<>();
    private final ReentrantReadWriteLock membershipLock = new ReentrantReadWriteLock();

    private final FailureDetector failureDetector;
    private final LeaderSelector leaderSelector = new LeaderSelector();
    private final ExecutorService virtualThreadExecutor = Executors.newVirtualThreadPerTaskExecutor();
    private final AtomicBoolean running = new AtomicBoolean(false);

    private Selector selector;
    private ServerSocketChannel serverChannel;
    private Thread selectorThread;
    private Thread heartbeatThread;
    private Thread syncThread;

    private volatile ClusterNode currentLeader;

    public ClusterManager(ClusterConfig config) {
        this.config = Objects.requireNonNull(config, "config cannot be null");
        
        // Add self to membership
        ClusterNode selfNode = new ClusterNode(config.nodeId(), config.host(), config.port(), NodeState.ACTIVE);
        this.membership.put(config.nodeId(), selfNode);
        this.currentLeader = selfNode;

        // Initialize Failure Detector
        this.failureDetector = new FailureDetector(config, membership, this::handleNodeStateChange);
    }

    /**
     * Starts cluster manager networks, discovery joiners, and background timers.
     */
    public synchronized void start() throws IOException {
        if (running.getAndSet(true)) {
            return;
        }

        // 1. Start Cluster NIO Server
        selector = Selector.open();
        serverChannel = ServerSocketChannel.open();
        serverChannel.configureBlocking(false);
        serverChannel.bind(new InetSocketAddress(config.host(), config.port()));
        serverChannel.register(selector, SelectionKey.OP_ACCEPT);

        selectorThread = new Thread(this::runSelectorLoop, "atlas-cluster-selector-" + config.nodeId());
        selectorThread.start();
        logger.info("Cluster Node '{}' server listening on cluster port: {}", config.nodeId(), config.port());

        // 2. Start Failure Detector
        failureDetector.start();

        // 3. Connect to Seed Nodes
        virtualThreadExecutor.submit(this::joinCluster);

        // 4. Start Periodic Heartbeat Loop
        heartbeatThread = Thread.ofVirtual().name("atlas-cluster-heartbeat-" + config.nodeId()).unstarted(this::runHeartbeatLoop);
        heartbeatThread.start();

        // 5. Start Periodic Anti-entropy Sync Gossip Loop
        syncThread = Thread.ofVirtual().name("atlas-cluster-sync-" + config.nodeId()).unstarted(this::runSyncGossipLoop);
        syncThread.start();
    }

    /**
     * Stops the cluster manager services and frees socket connections.
     */
    public synchronized void stop() {
        if (!running.getAndSet(false)) {
            return;
        }

        logger.info("Stopping Cluster Manager on Node '{}'...", config.nodeId());

        if (selector != null) {
            selector.wakeup();
        }

        try {
            if (serverChannel != null) {
                serverChannel.close();
            }
        } catch (IOException e) {
            logger.error("Error closing cluster server channel", e);
        }

        failureDetector.stop();
        virtualThreadExecutor.shutdownNow();

        // Join threads
        try {
            if (selectorThread != null) selectorThread.join(2000);
            if (heartbeatThread != null) heartbeatThread.join(2000);
            if (syncThread != null) syncThread.join(2000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        try {
            if (selector != null) {
                selector.close();
            }
        } catch (IOException e) {
            logger.error("Error closing selector", e);
        }

        logger.info("Cluster Manager stopped.");
    }

    /**
     * Resolves the current membership map snapshot.
     */
    public Map<String, ClusterNode> getMembership() {
        membershipLock.readLock().lock();
        try {
            return new HashMap<>(membership);
        } finally {
            membershipLock.readLock().unlock();
        }
    }

    /**
     * Retrieves the current coordinator leader node.
     */
    public ClusterNode getCurrentLeader() {
        return currentLeader;
    }

    private void runSelectorLoop() {
        while (running.get()) {
            try {
                int count = selector.select(1000);
                if (count == 0) {
                    continue;
                }

                Set<SelectionKey> selectedKeys = selector.selectedKeys();
                Iterator<SelectionKey> keyIterator = selectedKeys.iterator();

                while (keyIterator.hasNext()) {
                    SelectionKey key = keyIterator.next();
                    keyIterator.remove();

                    if (!key.isValid()) {
                        continue;
                    }

                    if (key.isAcceptable()) {
                        acceptConnection();
                    } else if (key.isReadable()) {
                        readPacket(key);
                    }
                }
            } catch (ClosedSelectorException cse) {
                break;
            } catch (Exception e) {
                logger.error("Error in selector loop", e);
            }
        }
    }

    private void acceptConnection() {
        try {
            SocketChannel client = serverChannel.accept();
            client.configureBlocking(false);
            client.register(selector, SelectionKey.OP_READ, ByteBuffer.allocate(8192));
        } catch (IOException e) {
            logger.error("Failed to accept client connection", e);
        }
    }

    private void readPacket(SelectionKey key) {
        SocketChannel client = (SocketChannel) key.channel();
        ByteBuffer buffer = (ByteBuffer) key.attachment();

        try {
            int read = client.read(buffer);
            if (read == -1) {
                closeChannel(client);
                return;
            }

            buffer.flip();
            while (true) {
                Packet packet;
                try {
                    packet = PacketCodec.decode(buffer);
                } catch (Exception e) {
                    logger.error("Failed to decode cluster packet", e);
                    closeChannel(client);
                    return;
                }

                if (packet == null) {
                    break;
                }

                virtualThreadExecutor.submit(() -> processClusterPacket(client, packet));
            }
            buffer.compact();
        } catch (IOException e) {
            closeChannel(client);
        }
    }

    private void processClusterPacket(SocketChannel client, Packet packet) {
        try {
            switch (packet.getType()) {
                case ClusterMessage.TYPE_JOIN -> {
                    ClusterNode joiner = ClusterMessage.decodeJoin(packet.getPayload());
                    logger.info("Received JOIN request from Node '{}' ({}:{})", 
                            joiner.getNodeId(), joiner.getHost(), joiner.getPort());

                    membershipLock.writeLock().lock();
                    try {
                        joiner.setState(NodeState.ACTIVE);
                        joiner.updateLastSeen();
                        membership.put(joiner.getNodeId(), joiner);
                        recalculateLeader();
                    } finally {
                        membershipLock.writeLock().unlock();
                    }

                    // Respond with current cluster membership table
                    List<ClusterNode> currentNodes;
                    membershipLock.readLock().lock();
                    try {
                        currentNodes = new ArrayList<>(membership.values());
                    } finally {
                        membershipLock.readLock().unlock();
                    }

                    byte[] respPayload = ClusterMessage.encodeMetadataSync(currentNodes);
                    sendPacketSync(client, new Packet(Packet.TYPE_RESP_SUCCESS, packet.getRequestId(), 0, respPayload));
                }
                case ClusterMessage.TYPE_HEARTBEAT -> {
                    String senderId = ClusterMessage.decodeHeartbeat(packet.getPayload());
                    membershipLock.writeLock().lock();
                    try {
                        ClusterNode node = membership.get(senderId);
                        if (node != null) {
                            node.updateLastSeen();
                            if (node.getState() == NodeState.SUSPECT || node.getState() == NodeState.DEAD) {
                                node.setState(NodeState.ACTIVE);
                                logger.info("Node '{}' returned to ACTIVE state.", senderId);
                                recalculateLeader();
                            }
                        } else {
                            logger.debug("Received heartbeat from unknown node '{}'. Ignoring.", senderId);
                        }
                    } finally {
                        membershipLock.writeLock().unlock();
                    }
                    sendPacketSync(client, new Packet(ClusterMessage.TYPE_HEARTBEAT_ACK, packet.getRequestId(), 0, ClusterMessage.encodeHeartbeat(config.nodeId())));
                }
                case ClusterMessage.TYPE_METADATA_SYNC -> {
                    List<ClusterNode> syncedNodes = ClusterMessage.decodeMetadataSync(packet.getPayload());
                    membershipLock.writeLock().lock();
                    try {
                        boolean modified = false;
                        for (ClusterNode syn : syncedNodes) {
                            if (syn.getNodeId().equalsIgnoreCase(config.nodeId())) {
                                continue;
                            }
                            ClusterNode existing = membership.get(syn.getNodeId());
                            if (existing == null) {
                                membership.put(syn.getNodeId(), syn);
                                modified = true;
                            } else if (syn.getLastSeenTimestamp() > existing.getLastSeenTimestamp()) {
                                existing.setState(syn.getState());
                                existing.setLastSeenTimestamp(syn.getLastSeenTimestamp());
                                modified = true;
                            }
                        }
                        if (modified) {
                            recalculateLeader();
                        }
                    } finally {
                        membershipLock.writeLock().unlock();
                    }
                    sendPacketSync(client, new Packet(Packet.TYPE_RESP_SUCCESS, packet.getRequestId(), 0, new byte[0]));
                }
            }
        } catch (Exception e) {
            logger.error("Error processing cluster packet", e);
        }
    }

    private void joinCluster() {
        for (String seed : config.seedNodes()) {
            if (!running.get()) {
                break;
            }

            String[] parts = seed.split(":");
            String seedHost = parts[0];
            int seedPort = Integer.parseInt(parts[1]);

            // Skip self
            if (seedHost.equalsIgnoreCase(config.host()) && seedPort == config.port()) {
                continue;
            }

            logger.info("Attempting to join cluster seed node at {}:{}", seedHost, seedPort);

            try (TcpClient client = new TcpClient(seedHost, seedPort, 3000)) {
                client.connect();
                byte[] joinPayload = ClusterMessage.encodeJoin(config.nodeId(), config.host(), config.port());
                Packet response = client.send(new Packet(ClusterMessage.TYPE_JOIN, 1, 0, joinPayload));

                if (response.getType() == Packet.TYPE_RESP_SUCCESS) {
                    List<ClusterNode> synced = ClusterMessage.decodeMetadataSync(response.getPayload());
                    membershipLock.writeLock().lock();
                    try {
                        for (ClusterNode n : synced) {
                            if (!n.getNodeId().equalsIgnoreCase(config.nodeId())) {
                                membership.put(n.getNodeId(), n);
                            }
                        }
                        recalculateLeader();
                    } finally {
                        membershipLock.writeLock().unlock();
                    }
                    logger.info("Successfully joined cluster via seed {}:{}", seedHost, seedPort);
                    return;
                }
            } catch (IOException e) {
                logger.warn("Failed to join cluster via seed node {}:{}: {}", seedHost, seedPort, e.getMessage());
            }
        }

        logger.info("Could not reach any seed nodes or list is empty. Operating in standalone cluster mode.");
    }

    private void runHeartbeatLoop() {
        while (running.get()) {
            try {
                Thread.sleep(config.heartbeatIntervalMs());

                List<ClusterNode> targets = new ArrayList<>();
                membershipLock.readLock().lock();
                try {
                    for (ClusterNode node : membership.values()) {
                        if (!node.getNodeId().equalsIgnoreCase(config.nodeId())) {
                            targets.add(node);
                        }
                    }
                } finally {
                    membershipLock.readLock().unlock();
                }

                for (ClusterNode target : targets) {
                    virtualThreadExecutor.submit(() -> sendHeartbeat(target));
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                logger.error("Error in heartbeat loop", e);
            }
        }
    }

    private void sendHeartbeat(ClusterNode target) {
        try (TcpClient client = new TcpClient(target.getHost(), target.getPort(), 2000)) {
            client.connect();
            byte[] hbPayload = ClusterMessage.encodeHeartbeat(config.nodeId());
            Packet response = client.send(new Packet(ClusterMessage.TYPE_HEARTBEAT, 999, 0, hbPayload));

            if (response.getType() == ClusterMessage.TYPE_HEARTBEAT_ACK) {
                membershipLock.writeLock().lock();
                try {
                    target.updateLastSeen();
                    if (target.getState() == NodeState.SUSPECT || target.getState() == NodeState.DEAD) {
                        target.setState(NodeState.ACTIVE);
                        logger.info("Peer Node '{}' recovered to ACTIVE.", target.getNodeId());
                        recalculateLeader();
                    }
                } finally {
                    membershipLock.writeLock().unlock();
                }
            }
        } catch (Exception e) {
            logger.debug("Failed sending heartbeat to Node '{}' at {}:{}", 
                    target.getNodeId(), target.getHost(), target.getPort());
        }
    }

    private void runSyncGossipLoop() {
        while (running.get()) {
            try {
                // Gossip membership tables every 2 * heartbeatInterval
                Thread.sleep(config.heartbeatIntervalMs() * 2);

                List<ClusterNode> targets = new ArrayList<>();
                List<ClusterNode> allNodes;
                membershipLock.readLock().lock();
                try {
                    allNodes = new ArrayList<>(membership.values());
                    for (ClusterNode n : allNodes) {
                        if (!n.getNodeId().equalsIgnoreCase(config.nodeId()) && n.getState() == NodeState.ACTIVE) {
                            targets.add(n);
                        }
                    }
                } finally {
                    membershipLock.readLock().unlock();
                }

                if (targets.isEmpty()) {
                    continue;
                }

                // Pick a random target node to gossip/sync metadata
                ClusterNode target = targets.get(ThreadLocalRandom.current().nextInt(targets.size()));
                virtualThreadExecutor.submit(() -> sendMetadataSync(target, allNodes));
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                logger.error("Error in sync gossip loop", e);
            }
        }
    }

    private void sendMetadataSync(ClusterNode target, List<ClusterNode> nodesList) {
        try (TcpClient client = new TcpClient(target.getHost(), target.getPort(), 3000)) {
            client.connect();
            byte[] payload = ClusterMessage.encodeMetadataSync(nodesList);
            client.send(new Packet(ClusterMessage.TYPE_METADATA_SYNC, 888, 0, payload));
        } catch (Exception e) {
            logger.debug("Sync gossip to Node '{}' failed: {}", target.getNodeId(), e.getMessage());
        }
    }

    private void handleNodeStateChange(ClusterNode node, NodeState oldState, NodeState newState) {
        logger.info("Node state changed callback - Node '{}': {} -> {}", node.getNodeId(), oldState, newState);
        membershipLock.writeLock().lock();
        try {
            recalculateLeader();
        } finally {
            membershipLock.writeLock().unlock();
        }
    }

    private void recalculateLeader() {
        ClusterNode newLeader = leaderSelector.selectLeader(membership);
        if (newLeader != null && !newLeader.equals(currentLeader)) {
            logger.info("Cluster coordinator transition: '{}' (old) -> '{}' (new)", 
                    currentLeader != null ? currentLeader.getNodeId() : "none", newLeader.getNodeId());
            currentLeader = newLeader;
        }
    }

    private void sendPacketSync(SocketChannel client, Packet packet) throws IOException {
        ByteBuffer buf = PacketCodec.encode(packet);
        while (buf.hasRemaining()) {
            client.write(buf);
        }
    }

    private void closeChannel(SocketChannel client) {
        try {
            client.close();
        } catch (IOException ignored) {}
    }
}
