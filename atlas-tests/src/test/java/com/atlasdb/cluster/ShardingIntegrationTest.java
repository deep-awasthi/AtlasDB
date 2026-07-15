package com.atlasdb.cluster;

import com.atlasdb.cluster.config.ClusterConfig;
import com.atlasdb.cluster.partition.HotPartitionDetector;
import com.atlasdb.cluster.partition.MigrationService;
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
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ShardingIntegrationTest {

    private final List<TcpServer> clientServers = new ArrayList<>();
    private final List<ClusterManager> clusterManagers = new ArrayList<>();
    private final List<HashStorageEngine<String, String>> storageEngines = new ArrayList<>();
    private final List<MigrationService> migrationServices = new ArrayList<>();
    private final List<PartitionManager> partitionManagers = new ArrayList<>();

    @BeforeEach
    void setUp() {
        clientServers.clear();
        clusterManagers.clear();
        storageEngines.clear();
        migrationServices.clear();
        partitionManagers.clear();
    }

    @AfterEach
    void tearDown() {
        for (ClusterManager m : clusterManagers) {
            m.stop();
        }
        for (TcpServer s : clientServers) {
            s.stop();
        }
        for (MigrationService ms : migrationServices) {
            ms.shutdown();
        }
    }

    private void bootNode(int id, List<String> seeds) throws IOException {
        int clientPort = 8900 + id;
        int clusterPort = 9900 + id;

        // 1. Storage Engine
        VersionGenerator vg = new VersionGenerator();
        HashStorageEngine<String, String> engine = new HashStorageEngine<>(new StorageConfig(4, 0.75f), vg);
        storageEngines.add(engine);

        // 2. Client TCP Data Server
        TcpServer clientServer = new TcpServer("127.0.0.1", clientPort, engine);
        clientServer.start();
        clientServers.add(clientServer);

        // 3. Cluster Manager & Partition Manager
        ClusterConfig clusterConfig = new ClusterConfig(
                "node" + id,
                "127.0.0.1",
                clusterPort,
                1000,
                3,
                2500,
                seeds
        );
        ClusterManager clusterManager = new ClusterManager(clusterConfig);
        clusterManager.start();
        clusterManagers.add(clusterManager);

        PartitionManager pm = new PartitionManager(clusterManager);
        partitionManagers.add(pm);

        // 4. Migration Service
        MigrationService ms = new MigrationService(clusterConfig, engine, pm);
        migrationServices.add(ms);
    }

    @Test
    void testConsistentHashingAndDynamicShardMigration() throws IOException, InterruptedException {
        // Boot Node 1 (Bootstrap seed)
        bootNode(1, List.of());
        
        // Boot Node 2 (pointing to Node 1 seed)
        bootNode(2, List.of("127.0.0.1:9901"));

        // Wait for cluster membership to sync
        Thread.sleep(1500);

        // Synchronize hash rings
        partitionManagers.get(0).updateRing();
        partitionManagers.get(1).updateRing();

        PartitionManager pm1 = partitionManagers.get(0);
        PartitionManager pm2 = partitionManagers.get(1);

        HashStorageEngine<String, String> engine1 = storageEngines.get(0);
        HashStorageEngine<String, String> engine2 = storageEngines.get(1);

        // Put keys on Node 1 directly
        // We will insert 20 test keys
        List<String> keysNode1ShouldKeep = new ArrayList<>();
        List<String> keysNode2ShouldAcquire = new ArrayList<>();

        for (int i = 0; i < 20; i++) {
            String key = "test-key-" + i;
            String val = "value-" + i;

            // Determine key owner according to consistent hash ring
            ClusterNode owner = pm1.getRoute(key);
            assertNotNull(owner);

            if (owner.getNodeId().equals("node1")) {
                keysNode1ShouldKeep.add(key);
            } else {
                keysNode2ShouldAcquire.add(key);
            }

            // Write all keys directly to Node 1 storage engine
            engine1.put(key, val);
        }

        // Assert Node 1 has all 20 keys initially, and Node 2 is empty
        assertEquals(20, engine1.size());
        assertEquals(0, engine2.size());

        // Trigger migration / rebalance from Node 1
        migrationServices.get(0).triggerRebalance();

        // Wait for virtual thread migration task to finish (scans and transmits keys via Tcp client requests)
        Thread.sleep(2000);

        // Verify keys were migrated to Node 2 correctly
        for (String key : keysNode2ShouldAcquire) {
            assertNull(engine1.get(key), "Key " + key + " should have been deleted from Node 1");
            assertNotNull(engine2.get(key), "Key " + key + " should have migrated to Node 2");
        }

        // Verify Node 1 kept its own keys
        for (String key : keysNode1ShouldKeep) {
            assertNotNull(engine1.get(key), "Key " + key + " should remain on Node 1");
            assertNull(engine2.get(key), "Key " + key + " should not be on Node 2");
        }

        // Verify total size sum remains 20 keys
        assertEquals(keysNode1ShouldKeep.size(), engine1.size());
        assertEquals(keysNode2ShouldAcquire.size(), engine2.size());
        assertEquals(20, engine1.size() + engine2.size());
    }

    @Test
    void testHotPartitionDetection() throws InterruptedException {
        // Test Hotspot tracking checks
        HotPartitionDetector detector = new HotPartitionDetector(15);
        detector.start();

        try {
            // Record access on table orders prefix 25 times
            for (int i = 0; i < 25; i++) {
                detector.recordAccess("table:orders:id-" + i);
            }
            
            // Record access on table users prefix 5 times (should not trigger warning)
            for (int i = 0; i < 5; i++) {
                detector.recordAccess("table:users:id-" + i);
            }

            // Sleep to trigger monitor sweep metrics checks
            Thread.sleep(1200);

        } finally {
            detector.stop();
        }
    }
}
