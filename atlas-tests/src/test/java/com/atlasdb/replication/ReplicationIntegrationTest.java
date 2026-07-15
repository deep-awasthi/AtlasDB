package com.atlasdb.replication;

import com.atlasdb.cluster.ClusterManager;
import com.atlasdb.cluster.config.ClusterConfig;
import com.atlasdb.cluster.partition.PartitionManager;
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

class ReplicationIntegrationTest {

    private final List<TcpServer> clientServers = new ArrayList<>();
    private final List<ClusterManager> clusterManagers = new ArrayList<>();
    private final List<HashStorageEngine<String, String>> storageEngines = new ArrayList<>();
    private final List<PartitionManager> partitionManagers = new ArrayList<>();
    private final List<ReplicationManager> replicationManagers = new ArrayList<>();

    @BeforeEach
    void setUp() {
        clientServers.clear();
        clusterManagers.clear();
        storageEngines.clear();
        partitionManagers.clear();
        replicationManagers.clear();
    }

    @AfterEach
    void tearDown() {
        for (ReplicationManager rm : replicationManagers) {
            rm.shutdown();
        }
        for (ClusterManager m : clusterManagers) {
            m.stop();
        }
        for (TcpServer s : clientServers) {
            s.stop();
        }
    }

    private void bootNode(int id, List<String> seeds) throws IOException {
        int clientPort = 8700 + id;
        int clusterPort = 9700 + id;

        // 1. Storage Engine
        VersionGenerator vg = new VersionGenerator();
        HashStorageEngine<String, String> engine = new HashStorageEngine<>(new StorageConfig(4, 0.75f), vg);
        storageEngines.add(engine);

        // 2. Client TCP Server
        TcpServer clientServer = new TcpServer("127.0.0.1", clientPort, engine);
        clientServer.start();
        clientServers.add(clientServer);

        // 3. Cluster & Partition Managers
        ClusterConfig clusterConfig = new ClusterConfig(
                "node" + id,
                "127.0.0.1",
                clusterPort,
                300,
                3,
                2500,
                seeds
        );
        ClusterManager clusterManager = new ClusterManager(clusterConfig);
        clusterManager.start();
        clusterManagers.add(clusterManager);

        PartitionManager pm = new PartitionManager(clusterManager);
        partitionManagers.add(pm);

        // 4. Replication Manager
        ReplicationManager rm = new ReplicationManager(clusterConfig, engine, clusterManager, pm, clientServer);
        replicationManagers.add(rm);
    }

    @Test
    void testQuorumReplicationAndReadRepair() throws IOException, InterruptedException {
        // Boot Node 1 (Bootstrap seed)
        bootNode(1, List.of());

        // Boot Node 2 and Node 3 pointing to Node 1 seed
        bootNode(2, List.of("127.0.0.1:9701"));
        bootNode(3, List.of("127.0.0.1:9701"));

        // Wait for cluster membership to sync
        Thread.sleep(1500);

        // Synchronize partition hash rings
        for (PartitionManager pm : partitionManagers) {
            pm.updateRing();
        }

        ReplicationManager rm1 = replicationManagers.get(0);
        ReplicationManager rm2 = replicationManagers.get(1);

        HashStorageEngine<String, String> engine1 = storageEngines.get(0);
        HashStorageEngine<String, String> engine2 = storageEngines.get(1);
        HashStorageEngine<String, String> engine3 = storageEngines.get(2);

        // 1. Perform Quorum Write
        long writeVersion = rm1.putReplicated("quorum-key", "quorum-value", ConsistencyLevel.QUORUM);
        assertTrue(writeVersion > 0);

        // Verify that data is present on all 3 replicas
        assertEquals("quorum-value", engine1.get("quorum-key"));
        assertEquals("quorum-value", engine2.get("quorum-key"));
        assertEquals("quorum-value", engine3.get("quorum-key"));

        // 2. Perform Quorum Read
        String readVal = rm2.getReplicated("quorum-key", ConsistencyLevel.QUORUM);
        assertEquals("quorum-value", readVal);

        // 3. Simulate Stale Data on Node 3 (inject older version version timestamp)
        engine3.put("quorum-key", "stale-value", 1000L);
        assertEquals("stale-value", engine3.get("quorum-key"));

        // Perform Quorum Read on Node 2 (triggers Read Repair asynchronously)
        String readValBeforeRepair = rm2.getReplicated("quorum-key", ConsistencyLevel.QUORUM);
        assertEquals("quorum-value", readValBeforeRepair, "Should resolve newest version");

        // Wait for asynchronous read repair task to finish (writes latest value to stale Node 3)
        boolean healed = false;
        for (int i = 0; i < 50; i++) {
            if ("quorum-value".equals(engine3.get("quorum-key"))) {
                healed = true;
                break;
            }
            Thread.sleep(100);
        }
        assertTrue(healed, "Node 3 should be healed by read repair");
    }

    @Test
    void testQuorumFailuresUnderNetworkPartition() throws IOException, InterruptedException {
        // Boot Node 1 (Bootstrap seed)
        bootNode(1, List.of());

        // Boot Node 2 and Node 3 pointing to Node 1 seed
        bootNode(2, List.of("127.0.0.1:9701"));
        bootNode(3, List.of("127.0.0.1:9701"));

        Thread.sleep(1500);

        for (PartitionManager pm : partitionManagers) {
            pm.updateRing();
        }

        ReplicationManager rm1 = replicationManagers.get(0);

        // Stop Node 2 and Node 3 to simulate partition loss (majority of replicas offline)
        clusterManagers.get(1).stop();
        clientServers.get(1).stop();
        clusterManagers.get(2).stop();
        clientServers.get(2).stop();

        // Give cluster failure detector time to update states
        Thread.sleep(3000);

        // Update Node 1 partition ring
        partitionManagers.get(0).updateRing();

        // 1. Quorum Write should fail (requires at least 2 active replicas, only 1 is active)
        assertThrows(RuntimeException.class, () -> {
            rm1.putReplicated("partition-key", "partition-val", ConsistencyLevel.QUORUM);
        }, "Quorum write must fail if majority is offline");

        // 2. Consistency ONE Write should succeed (requires only 1 active replica, which is Node 1 itself)
        long oneVersion = rm1.putReplicated("partition-key", "one-val", ConsistencyLevel.ONE);
        assertTrue(oneVersion > 0);
        assertEquals("one-val", storageEngines.get(0).get("partition-key"));
    }
}
