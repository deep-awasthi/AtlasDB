package com.atlasdb.raft;

import com.atlasdb.cluster.ClusterManager;
import com.atlasdb.cluster.ClusterNode;
import com.atlasdb.cluster.NodeState;
import com.atlasdb.cluster.config.ClusterConfig;
import com.atlasdb.network.client.TcpClient;
import com.atlasdb.network.protocol.Packet;
import com.atlasdb.network.server.TcpServer;
import com.atlasdb.raft.log.RaftLogEntry;
import com.atlasdb.storage.StorageEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Raft Consensus node engine. Coordinates leader election and log replication.
 */
public final class RaftNode {

    private static final Logger logger = LoggerFactory.getLogger(RaftNode.class);

    // Custom Raft Packets
    public static final byte TYPE_RAFT_REQUEST_VOTE = 0x30;
    public static final byte TYPE_RAFT_APPEND_ENTRIES = 0x31;

    private final ClusterConfig config;
    private final StorageEngine<String, String> storageEngine;
    private final ClusterManager clusterManager;
    private final TcpServer tcpServer;

    private final List<RaftLogEntry> log = new ArrayList<>();
    private final ReentrantLock stateLock = new ReentrantLock();
    private final ExecutorService virtualThreadExecutor = Executors.newVirtualThreadPerTaskExecutor();
    private final AtomicBoolean running = new AtomicBoolean(false);

    // Raft persistent state (in-memory)
    private long currentTerm = 0;
    private String votedFor = null;
    
    // Raft volatile state
    private long commitIndex = 0;
    private long lastApplied = 0;
    private RaftRole role = RaftRole.FOLLOWER;

    // Leader state
    private final Map<String, Long> nextIndex = new HashMap<>();
    private final Map<String, Long> matchIndex = new HashMap<>();

    private final long electionTimeoutMs;
    private volatile long lastHeartbeatTime = System.currentTimeMillis();

    private Thread electionTimeoutThread;
    private Thread heartbeatThread;

    public RaftNode(ClusterConfig config, StorageEngine<String, String> storageEngine,
                    ClusterManager clusterManager, TcpServer tcpServer, long electionTimeoutMs) {
        this.config = Objects.requireNonNull(config, "config cannot be null");
        this.storageEngine = Objects.requireNonNull(storageEngine, "storageEngine cannot be null");
        this.clusterManager = Objects.requireNonNull(clusterManager, "clusterManager cannot be null");
        this.tcpServer = Objects.requireNonNull(tcpServer, "tcpServer cannot be null");
        this.electionTimeoutMs = electionTimeoutMs;

        // Register custom packet handler hooks with TcpServer
        this.tcpServer.registerHandler(this::handleCustomPacket);
    }

    /**
     * Boots the Raft node, starts timeouts and heartbeat timers.
     */
    public synchronized void start() {
        if (running.getAndSet(true)) {
            return;
        }

        logger.info("Starting Raft Node '{}'...", config.nodeId());
        lastHeartbeatTime = System.currentTimeMillis();

        electionTimeoutThread = Thread.ofVirtual().name("raft-election-" + config.nodeId()).unstarted(this::runElectionTimeoutLoop);
        electionTimeoutThread.start();

        heartbeatThread = Thread.ofVirtual().name("raft-heartbeat-" + config.nodeId()).unstarted(this::runHeartbeatLoop);
        heartbeatThread.start();
    }

    /**
     * Stops the Raft node services.
     */
    public synchronized void stop() {
        if (!running.getAndSet(false)) {
            return;
        }

        logger.info("Stopping Raft Node '{}'...", config.nodeId());

        if (electionTimeoutThread != null) electionTimeoutThread.interrupt();
        if (heartbeatThread != null) heartbeatThread.interrupt();

        virtualThreadExecutor.shutdownNow();

        try {
            if (electionTimeoutThread != null) electionTimeoutThread.join(2000);
            if (heartbeatThread != null) heartbeatThread.join(2000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public RaftRole getRole() {
        stateLock.lock();
        try {
            return role;
        } finally {
            stateLock.unlock();
        }
    }

    public long getCurrentTerm() {
        stateLock.lock();
        try {
            return currentTerm;
        } finally {
            stateLock.unlock();
        }
    }

    public long getCommitIndex() {
        stateLock.lock();
        try {
            return commitIndex;
        } finally {
            stateLock.unlock();
        }
    }

    public int getLogSize() {
        stateLock.lock();
        try {
            return log.size();
        } finally {
            stateLock.unlock();
        }
    }

    /**
     * Executes a client write request under consensus.
     * Maps to local appends if Leader, or fails if not.
     */
    public boolean putConsensus(String key, String value) {
        long targetIndex;
        stateLock.lock();
        try {
            if (role != RaftRole.LEADER) {
                return false; // Not leader
            }
            targetIndex = log.size() + 1;
            RaftLogEntry entry = new RaftLogEntry(targetIndex, currentTerm, key, value, false);
            log.add(entry);
            logger.debug("Leader Node '{}' appended local write key '{}' at index {}", config.nodeId(), key, targetIndex);
        } finally {
            stateLock.unlock();
        }

        // Trigger immediate replication
        virtualThreadExecutor.submit(this::sendHeartbeats);

        // Wait for commit
        long start = System.currentTimeMillis();
        while (System.currentTimeMillis() - start < 4000) {
            stateLock.lock();
            try {
                if (commitIndex >= targetIndex) {
                    return true;
                }
                if (role != RaftRole.LEADER) {
                    return false; // Stepped down during write wait
                }
            } finally {
                stateLock.unlock();
            }
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }

        return false;
    }

    private void runElectionTimeoutLoop() {
        Random rand = new Random();
        while (running.get()) {
            long timeout = electionTimeoutMs + rand.nextInt(300);
            try {
                Thread.sleep(timeout);
                stateLock.lock();
                try {
                    if (role != RaftRole.LEADER) {
                        long now = System.currentTimeMillis();
                        if (now - lastHeartbeatTime >= timeout) {
                            logger.info("Election timeout expired. Starting election on Node '{}' for term {}", 
                                    config.nodeId(), currentTerm + 1);
                            startElection();
                        }
                    }
                } finally {
                    stateLock.unlock();
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    private void startElection() {
        role = RaftRole.CANDIDATE;
        currentTerm++;
        votedFor = config.nodeId();
        lastHeartbeatTime = System.currentTimeMillis();

        List<ClusterNode> peers = getPeers();
        int totalNodes = peers.size() + 1;
        AtomicInteger votesGranted = new AtomicInteger(1); // voted for self

        long term = currentTerm;
        long lastLogIndex = log.size();
        long lastLogTerm = lastLogIndex > 0 ? log.get((int) lastLogIndex - 1).term() : 0;

        for (ClusterNode peer : peers) {
            virtualThreadExecutor.submit(() -> {
                boolean granted = sendRequestVote(peer, term, config.nodeId(), lastLogIndex, lastLogTerm);
                if (granted) {
                    int votes = votesGranted.incrementAndGet();
                    if (votes >= (totalNodes / 2) + 1) {
                        stateLock.lock();
                        try {
                            if (role == RaftRole.CANDIDATE && currentTerm == term) {
                                logger.info("Node '{}' won election for term {}", config.nodeId(), currentTerm);
                                becomeLeader();
                            }
                        } finally {
                            stateLock.unlock();
                        }
                    }
                }
            });
        }
    }

    private void becomeLeader() {
        role = RaftRole.LEADER;
        List<ClusterNode> peers = getPeers();
        long lastLogIdx = log.size();
        for (ClusterNode peer : peers) {
            nextIndex.put(peer.getNodeId(), lastLogIdx + 1);
            matchIndex.put(peer.getNodeId(), 0L);
        }
        sendHeartbeats();
    }

    private void runHeartbeatLoop() {
        while (running.get()) {
            try {
                Thread.sleep(100);
                stateLock.lock();
                try {
                    if (role == RaftRole.LEADER) {
                        sendHeartbeats();
                    }
                } finally {
                    stateLock.unlock();
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    private void sendHeartbeats() {
        List<ClusterNode> peers = getPeers();
        for (ClusterNode peer : peers) {
            virtualThreadExecutor.submit(() -> {
                long term;
                long prevLogIdx;
                long prevLogTerm;
                long commitIdx;
                List<RaftLogEntry> entriesToSend = new ArrayList<>();

                stateLock.lock();
                try {
                    if (role != RaftRole.LEADER) {
                        return;
                    }
                    term = currentTerm;
                    long nextIdx = nextIndex.getOrDefault(peer.getNodeId(), 1L);
                    prevLogIdx = nextIdx - 1;
                    prevLogTerm = prevLogIdx > 0 ? log.get((int) prevLogIdx - 1).term() : 0;
                    commitIdx = commitIndex;

                    for (int i = (int) prevLogIdx; i < log.size(); i++) {
                        entriesToSend.add(log.get(i));
                    }
                } finally {
                    stateLock.unlock();
                }

                AppendEntriesResult res = sendAppendEntries(peer, term, config.nodeId(), prevLogIdx, prevLogTerm, entriesToSend, commitIdx);
                if (res != null) {
                    stateLock.lock();
                    try {
                        if (role == RaftRole.LEADER && currentTerm == term) {
                            if (res.success) {
                                long lastSentIdx = prevLogIdx + entriesToSend.size();
                                nextIndex.put(peer.getNodeId(), lastSentIdx + 1);
                                matchIndex.put(peer.getNodeId(), lastSentIdx);
                                updateLeaderCommitIndex();
                            } else {
                                if (res.term > currentTerm) {
                                    stepDown(res.term);
                                } else {
                                    // Decrement nextIndex and retry later
                                    long nextIdx = nextIndex.getOrDefault(peer.getNodeId(), 1L);
                                    if (nextIdx > 1) {
                                        nextIndex.put(peer.getNodeId(), nextIdx - 1);
                                    }
                                }
                            }
                        }
                    } finally {
                        stateLock.unlock();
                    }
                }
            });
        }
    }

    private void updateLeaderCommitIndex() {
        List<ClusterNode> peers = getPeers();
        int totalNodes = peers.size() + 1;
        long maxCommitIdx = commitIndex;

        for (long i = commitIndex + 1; i <= log.size(); i++) {
            long idxToCheck = i;
            long termOfIndex = log.get((int) idxToCheck - 1).term();
            if (termOfIndex == currentTerm) {
                int matchCount = 1;
                for (ClusterNode peer : peers) {
                    if (matchIndex.getOrDefault(peer.getNodeId(), 0L) >= idxToCheck) {
                        matchCount++;
                    }
                }
                if (matchCount >= (totalNodes / 2) + 1) {
                    maxCommitIdx = idxToCheck;
                }
            }
        }

        if (maxCommitIdx > commitIndex) {
            commitIndex = maxCommitIdx;
            applyEntriesToStateMachine();
        }
    }

    private void applyEntriesToStateMachine() {
        while (lastApplied < commitIndex) {
            lastApplied++;
            RaftLogEntry entry = log.get((int) lastApplied - 1);
            if (entry.isDelete()) {
                storageEngine.delete(entry.key(), entry.term());
            } else {
                storageEngine.put(entry.key(), entry.value(), entry.term());
            }
            logger.debug("Applied log index {} (key '{}') to State Machine on Node '{}'", 
                    lastApplied, entry.key(), config.nodeId());
        }
    }

    private void stepDown(long term) {
        role = RaftRole.FOLLOWER;
        currentTerm = term;
        votedFor = null;
        lastHeartbeatTime = System.currentTimeMillis();
    }

    private List<ClusterNode> getPeers() {
        Map<String, ClusterNode> members = clusterManager.getMembership();
        List<ClusterNode> peers = new ArrayList<>();
        for (ClusterNode n : members.values()) {
            if (!n.getNodeId().equalsIgnoreCase(config.nodeId()) && n.getState() != NodeState.DEAD) {
                peers.add(n);
            }
        }
        return peers;
    }

    private boolean sendRequestVote(ClusterNode peer, long term, String candidateId, long lastLogIndex, long lastLogTerm) {
        int clientPort = peer.getPort() - 1000;
        try (TcpClient client = new TcpClient(peer.getHost(), clientPort, 2000)) {
            client.connect();

            byte[] idBytes = candidateId.getBytes(StandardCharsets.UTF_8);
            ByteBuffer buf = ByteBuffer.allocate(8 + 4 + idBytes.length + 8 + 8);
            buf.putLong(term);
            buf.putInt(idBytes.length);
            buf.put(idBytes);
            buf.putLong(lastLogIndex);
            buf.putLong(lastLogTerm);
            buf.flip();

            Packet response = client.send(new Packet(TYPE_RAFT_REQUEST_VOTE, 10, 0, buf.array()));
            if (response.getType() == Packet.TYPE_RESP_SUCCESS) {
                ByteBuffer respBuf = ByteBuffer.wrap(response.getPayload());
                long respTerm = respBuf.getLong();
                boolean granted = respBuf.get() == 1;

                stateLock.lock();
                try {
                    if (respTerm > currentTerm) {
                        stepDown(respTerm);
                    }
                } finally {
                    stateLock.unlock();
                }

                return granted;
            }
        } catch (Exception e) {
            logger.debug("RequestVote to Node '{}' failed: {}", peer.getNodeId(), e.getMessage());
        }
        return false;
    }

    private AppendEntriesResult sendAppendEntries(ClusterNode peer, long term, String leaderId,
                                                  long prevLogIndex, long prevLogTerm,
                                                  List<RaftLogEntry> entries, long leaderCommit) {
        int clientPort = peer.getPort() - 1000;
        try (TcpClient client = new TcpClient(peer.getHost(), clientPort, 2000)) {
            client.connect();

            byte[] leaderIdBytes = leaderId.getBytes(StandardCharsets.UTF_8);

            // Compute payload size
            int size = 8 + 4 + leaderIdBytes.length + 8 + 8 + 8 + 4;
            for (RaftLogEntry entry : entries) {
                byte[] keyBytes = entry.key().getBytes(StandardCharsets.UTF_8);
                byte[] valBytes = entry.value() == null ? new byte[0] : entry.value().getBytes(StandardCharsets.UTF_8);
                size += 8 + 8 + 1 + 4 + keyBytes.length + 4 + valBytes.length;
            }

            ByteBuffer buf = ByteBuffer.allocate(size);
            buf.putLong(term);
            buf.putInt(leaderIdBytes.length);
            buf.put(leaderIdBytes);
            buf.putLong(prevLogIndex);
            buf.putLong(prevLogTerm);
            buf.putLong(leaderCommit);
            buf.putInt(entries.size());

            for (RaftLogEntry entry : entries) {
                buf.putLong(entry.index());
                buf.putLong(entry.term());
                buf.put((byte) (entry.isDelete() ? 1 : 0));
                byte[] keyBytes = entry.key().getBytes(StandardCharsets.UTF_8);
                byte[] valBytes = entry.value() == null ? new byte[0] : entry.value().getBytes(StandardCharsets.UTF_8);
                buf.putInt(keyBytes.length);
                buf.put(keyBytes);
                buf.putInt(valBytes.length);
                buf.put(valBytes);
            }
            buf.flip();

            Packet response = client.send(new Packet(TYPE_RAFT_APPEND_ENTRIES, 20, 0, buf.array()));
            if (response.getType() == Packet.TYPE_RESP_SUCCESS) {
                ByteBuffer respBuf = ByteBuffer.wrap(response.getPayload());
                long respTerm = respBuf.getLong();
                boolean success = respBuf.get() == 1;
                return new AppendEntriesResult(respTerm, success);
            }
        } catch (Exception e) {
            logger.debug("AppendEntries to Node '{}' failed: {}", peer.getNodeId(), e.getMessage());
        }
        return null;
    }

    private boolean handleCustomPacket(SocketChannel client, Packet packet) throws IOException {
        if (packet.getType() == TYPE_RAFT_REQUEST_VOTE) {
            ByteBuffer buf = ByteBuffer.wrap(packet.getPayload());
            long term = buf.getLong();
            int candidateIdLen = buf.getInt();
            byte[] idBytes = new byte[candidateIdLen];
            buf.get(idBytes);
            String candidateId = new String(idBytes, StandardCharsets.UTF_8);
            long lastLogIndex = buf.getLong();
            long lastLogTerm = buf.getLong();

            stateLock.lock();
            boolean voteGranted = false;
            try {
                if (term > currentTerm) {
                    stepDown(term);
                }

                if (term == currentTerm && (votedFor == null || votedFor.equalsIgnoreCase(candidateId))) {
                    long localLastIdx = log.size();
                    long localLastTerm = localLastIdx > 0 ? log.get((int) localLastIdx - 1).term() : 0;

                    boolean upToDate = (lastLogTerm > localLastTerm) || 
                            (lastLogTerm == localLastTerm && lastLogIndex >= localLastIdx);

                    if (upToDate) {
                        voteGranted = true;
                        votedFor = candidateId;
                        lastHeartbeatTime = System.currentTimeMillis();
                    }
                }

                ByteBuffer resp = ByteBuffer.allocate(8 + 1);
                resp.putLong(currentTerm);
                resp.put((byte) (voteGranted ? 1 : 0));
                resp.flip();

                tcpServer.sendPacket(client, new Packet(Packet.TYPE_RESP_SUCCESS, packet.getRequestId(), 0, resp.array()));
            } finally {
                stateLock.unlock();
            }
            return true;
        }

        if (packet.getType() == TYPE_RAFT_APPEND_ENTRIES) {
            ByteBuffer buf = ByteBuffer.wrap(packet.getPayload());
            long term = buf.getLong();
            int leaderIdLen = buf.getInt();
            byte[] leaderIdBytes = new byte[leaderIdLen];
            buf.get(leaderIdBytes);
            String leaderId = new String(leaderIdBytes, StandardCharsets.UTF_8);
            long prevLogIndex = buf.getLong();
            long prevLogTerm = buf.getLong();
            long leaderCommit = buf.getLong();
            int entriesCount = buf.getInt();

            List<RaftLogEntry> incomingEntries = new ArrayList<>();
            for (int i = 0; i < entriesCount; i++) {
                long index = buf.getLong();
                long entryTerm = buf.getLong();
                boolean isDelete = buf.get() == 1;

                int keyLen = buf.getInt();
                byte[] keyBytes = new byte[keyLen];
                buf.get(keyBytes);
                String key = new String(keyBytes, StandardCharsets.UTF_8);

                int valLen = buf.getInt();
                String val = null;
                if (valLen > 0) {
                    byte[] valBytes = new byte[valLen];
                    buf.get(valBytes);
                    val = new String(valBytes, StandardCharsets.UTF_8);
                }
                incomingEntries.add(new RaftLogEntry(index, entryTerm, key, val, isDelete));
            }

            stateLock.lock();
            boolean success = false;
            try {
                if (term > currentTerm) {
                    stepDown(term);
                }

                if (term == currentTerm) {
                    lastHeartbeatTime = System.currentTimeMillis();
                    if (role != RaftRole.FOLLOWER) {
                        role = RaftRole.FOLLOWER;
                    }

                    // Check log consistency at prevLogIndex
                    boolean consistent = true;
                    if (prevLogIndex > 0) {
                        if (log.size() < prevLogIndex) {
                            consistent = false;
                        } else if (log.get((int) prevLogIndex - 1).term() != prevLogTerm) {
                            consistent = false;
                        }
                    }

                    if (consistent) {
                        success = true;
                        
                        // Append entries
                        for (RaftLogEntry entry : incomingEntries) {
                            if (entry.index() <= log.size()) {
                                // Conflict check
                                if (log.get((int) entry.index() - 1).term() != entry.term()) {
                                    log.subList((int) entry.index() - 1, log.size()).clear();
                                    log.add(entry);
                                }
                            } else {
                                log.add(entry);
                            }
                        }

                        // Update commitIndex
                        if (leaderCommit > commitIndex) {
                            commitIndex = Math.min(leaderCommit, log.size());
                            applyEntriesToStateMachine();
                        }
                    }
                }

                ByteBuffer resp = ByteBuffer.allocate(8 + 1);
                resp.putLong(currentTerm);
                resp.put((byte) (success ? 1 : 0));
                resp.flip();

                tcpServer.sendPacket(client, new Packet(Packet.TYPE_RESP_SUCCESS, packet.getRequestId(), 0, resp.array()));
            } finally {
                stateLock.unlock();
            }
            return true;
        }

        return false;
    }

    private static class AppendEntriesResult {
        final long term;
        final boolean success;

        AppendEntriesResult(long term, boolean success) {
            this.term = term;
            this.success = success;
        }
    }
}
