package com.atlasdb.raft;

import com.atlasdb.cluster.ClusterManager;
import com.atlasdb.cluster.config.ClusterConfig;
import com.atlasdb.common.VersionGenerator;
import com.atlasdb.network.server.TcpServer;
import com.atlasdb.storage.HashStorageEngine;
import com.atlasdb.storage.config.StorageConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class RaftIntegrationTest {

    private final List<TcpServer> clientServers = new ArrayList<>();
    private final List<ClusterManager> clusterManagers = new ArrayList<>();
    private final List<HashStorageEngine<String, String>> storageEngines = new ArrayList<>();
    private final List<RaftNode> raftNodes = new ArrayList<>();

    @BeforeEach
    void setUp() {
        clientServers.clear();
        clusterManagers.clear();
        storageEngines.clear();
        raftNodes.clear();
    }

    @AfterEach
    void tearDown() {
        for (RaftNode rn : raftNodes) {
            rn.stop();
        }
        for (ClusterManager cm : clusterManagers) {
            cm.stop();
        }
        for (TcpServer s : clientServers) {
            s.stop();
        }
    }

    private void bootNode(int id, List<String> seeds) throws IOException {
        int clientPort = 8600 + id;
        int clusterPort = 9600 + id;

        // 1. Storage Engine
        VersionGenerator vg = new VersionGenerator();
        HashStorageEngine<String, String> engine = new HashStorageEngine<>(new StorageConfig(4, 0.75f), vg);
        storageEngines.add(engine);

        // 2. Client TCP Server
        TcpServer clientServer = new TcpServer("127.0.0.1", clientPort, engine);
        clientServer.start();
        clientServers.add(clientServer);

        // 3. Cluster Manager
        ClusterConfig clusterConfig = new ClusterConfig(
                "node" + id,
                "127.0.0.1",
                clusterPort,
                300,  // short heartbeats for quick test election transitions
                3,
                2500,
                seeds
        );
        ClusterManager clusterManager = new ClusterManager(clusterConfig);
        clusterManager.start();
        clusterManagers.add(clusterManager);

        // 4. Raft Node (Election timeout 500ms)
        RaftNode raftNode = new RaftNode(clusterConfig, engine, clusterManager, clientServer, 500);
        raftNode.start();
        raftNodes.add(raftNode);
    }

    @Test
    void testRaftElectionsLogReplicationAndFailover() throws IOException, InterruptedException {
        // Boot Node 1 (Bootstrap seed)
        bootNode(1, List.of());

        // Boot Node 2 and Node 3 pointing to Node 1 seed
        bootNode(2, List.of("127.0.0.1:9601"));
        bootNode(3, List.of("127.0.0.1:9601"));

        // Wait for election convergence (election timeouts are ~500ms-800ms)
        Thread.sleep(3000);

        // 1. Assert exactly one leader is elected
        RaftNode leader = null;
        int leaderCount = 0;
        int followerCount = 0;

        for (RaftNode rn : raftNodes) {
            if (rn.getRole() == RaftRole.LEADER) {
                leader = rn;
                leaderCount++;
            } else if (rn.getRole() == RaftRole.FOLLOWER) {
                followerCount++;
            }
        }

        assertEquals(1, leaderCount, "Exactly one leader must be elected");
        assertEquals(2, followerCount, "The other two nodes must be followers");
        assertNotNull(leader, "Leader cannot be null");

        // 2. Perform write on the leader
        boolean writeOk = leader.putConsensus("consensus-key", "consensus-val");
        assertTrue(writeOk, "Consensus write on leader should succeed");

        // Assert leader applied write to state machine
        int leaderIdx = raftNodes.indexOf(leader);
        assertEquals("consensus-val", storageEngines.get(leaderIdx).get("consensus-key"));

        // Wait for replication to followers
        Thread.sleep(500);

        // Verify write replicated to followers state machines
        for (int i = 0; i < 3; i++) {
            assertEquals("consensus-val", storageEngines.get(i).get("consensus-key"), 
                    "State machine at node index " + i + " must match consensus value");
        }

        // 3. Fail the Leader
        loggerInfo("Simulating crash of leader: " + leader.getRole() + " (index " + leaderIdx + ")");
        leader.stop();
        clusterManagers.get(leaderIdx).stop();
        clientServers.get(leaderIdx).stop();

        // Wait for election timeout and new leader election
        Thread.sleep(3500);

        RaftNode newLeader = null;
        int newLeaderCount = 0;
        int newFollowerCount = 0;

        for (int i = 0; i < 3; i++) {
            if (i == leaderIdx) {
                continue; // Skip crashed node
            }
            RaftNode rn = raftNodes.get(i);
            if (rn.getRole() == RaftRole.LEADER) {
                newLeader = rn;
                newLeaderCount++;
            } else if (rn.getRole() == RaftRole.FOLLOWER) {
                newFollowerCount++;
            }
        }

        assertEquals(1, newLeaderCount, "New leader must be elected from the active partition");
        assertEquals(1, newFollowerCount, "Remaining active node must be follower");
        assertNotNull(newLeader, "New leader cannot be null");

        // 4. Perform write on new leader
        boolean newWriteOk = newLeader.putConsensus("new-consensus-key", "new-consensus-val");
        assertTrue(newWriteOk, "Consensus write on new leader should succeed");

        // Wait for replication to active follower
        Thread.sleep(500);

        int newLeaderIdx = raftNodes.indexOf(newLeader);
        for (int i = 0; i < 3; i++) {
            if (i == leaderIdx) {
                // Crashed node should not have it yet
                assertNull(storageEngines.get(i).get("new-consensus-key"));
                continue;
            }
            assertEquals("new-consensus-val", storageEngines.get(i).get("new-consensus-key"));
        }

        // 5. Restart the old leader to test recovery catchup
        loggerInfo("Recovering crashed old leader...");
        int crashedId = leaderIdx + 1;
        int crashedClientPort = 8600 + crashedId;
        int crashedClusterPort = 9600 + crashedId;

        // Re-instantiate storage engine (empty or we can reuse existing memory storage)
        // Re-injecting crashed managers
        HashStorageEngine<String, String> recoveredEngine = storageEngines.get(leaderIdx);
        // Clear it to verify catchup from log replication
        recoveredEngine.clear();
        assertNull(recoveredEngine.get("consensus-key"));

        TcpServer recoveredServer = new TcpServer("127.0.0.1", crashedClientPort, recoveredEngine);
        recoveredServer.start();
        clientServers.set(leaderIdx, recoveredServer);

        ClusterConfig recoveredClusterConfig = new ClusterConfig(
                "node" + crashedId,
                "127.0.0.1",
                crashedClusterPort,
                300,
                3,
                2500,
                List.of("127.0.0.1:9601", "127.0.0.1:9602", "127.0.0.1:9603")
        );
        ClusterManager recoveredClusterManager = new ClusterManager(recoveredClusterConfig);
        recoveredClusterManager.start();
        clusterManagers.set(leaderIdx, recoveredClusterManager);

        RaftNode recoveredRaftNode = new RaftNode(recoveredClusterConfig, recoveredEngine, recoveredClusterManager, recoveredServer, 500);
        recoveredRaftNode.start();
        raftNodes.set(leaderIdx, recoveredRaftNode);

        // Wait for recovery sync and log replication catchup
        Thread.sleep(3000);

        // Verify recovered old leader caught up and synchronized its state machine
        assertEquals("consensus-val", recoveredEngine.get("consensus-key"), "Recovered node should catch up consensus-key");
        assertEquals("new-consensus-val", recoveredEngine.get("new-consensus-key"), "Recovered node should catch up new-consensus-key");
    }

    private void loggerInfo(String msg) {
        System.out.println("[INFO] " + msg);
    }
}
