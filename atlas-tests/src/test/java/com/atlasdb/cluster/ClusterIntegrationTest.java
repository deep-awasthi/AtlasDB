package com.atlasdb.cluster;

import com.atlasdb.cluster.config.ClusterConfig;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ClusterIntegrationTest {

    @Test
    void testClusterDiscoveryHeartbeatAndFailover() throws IOException, InterruptedException {
        // Setup 3 nodes configs
        ClusterConfig config1 = new ClusterConfig(
                "node1",
                "127.0.0.1",
                9901,
                1000, // heartbeat interval
                3,    // failure threshold
                2500, // suspect timeout
                List.of()
        );

        ClusterConfig config2 = new ClusterConfig(
                "node2",
                "127.0.0.1",
                9902,
                1000,
                3,
                2500,
                List.of("127.0.0.1:9901")
        );

        ClusterConfig config3 = new ClusterConfig(
                "node3",
                "127.0.0.1",
                9903,
                1000,
                3,
                2500,
                List.of("127.0.0.1:9901")
        );

        // Instantiate cluster managers
        ClusterManager manager1 = new ClusterManager(config1);
        ClusterManager manager2 = new ClusterManager(config2);
        ClusterManager manager3 = new ClusterManager(config3);

        try {
            // 1. Start Node 1 (Bootstrap seed)
            manager1.start();
            Thread.sleep(200);

            // Verify manager1 is coordinator of itself
            assertEquals("node1", manager1.getCurrentLeader().getNodeId());

            // 2. Start Node 2 and Node 3
            manager2.start();
            manager3.start();

            // Give them time to join, sync, and swap pings
            Thread.sleep(1500);

            // Assert they all discovered each other
            Map<String, ClusterNode> members1 = manager1.getMembership();
            Map<String, ClusterNode> members2 = manager2.getMembership();
            Map<String, ClusterNode> members3 = manager3.getMembership();

            assertEquals(3, members1.size(), "Node 1 should know 3 nodes");
            assertEquals(3, members2.size(), "Node 2 should know 3 nodes");
            assertEquals(3, members3.size(), "Node 3 should know 3 nodes");

            // Verify they agree on the coordinator leader ("node1" is lexicographically smallest)
            assertEquals("node1", manager1.getCurrentLeader().getNodeId());
            assertEquals("node1", manager2.getCurrentLeader().getNodeId());
            assertEquals("node1", manager3.getCurrentLeader().getNodeId());

            // Verify states are ACTIVE
            assertEquals(NodeState.ACTIVE, members1.get("node2").getState());
            assertEquals(NodeState.ACTIVE, members1.get("node3").getState());

            // 3. Stop Node 1 to simulate crash
            manager1.stop();
            loggerInfo("Simulated crash of node1.");

            // Wait for suspect and dead timeouts (suspect timeout is 2500ms)
            Thread.sleep(4000);

            // Verify node2 and node3 detected node1 is DEAD
            Map<String, ClusterNode> members2AfterCrash = manager2.getMembership();
            Map<String, ClusterNode> members3AfterCrash = manager3.getMembership();

            assertEquals(NodeState.DEAD, members2AfterCrash.get("node1").getState());
            assertEquals(NodeState.DEAD, members3AfterCrash.get("node1").getState());

            // Verify failover happened: "node2" must be the new coordinator (lowest lexicographical active ID)
            assertEquals("node2", manager2.getCurrentLeader().getNodeId());
            assertEquals("node2", manager3.getCurrentLeader().getNodeId());

            // 4. Restart Node 1 to test rejoin recovery
            loggerInfo("Restarting node1...");
            // Use config with seed to allow it to contact others
            ClusterConfig config1WithSeeds = new ClusterConfig(
                    "node1",
                    "127.0.0.1",
                    9901,
                    1000,
                    3,
                    2500,
                    List.of("127.0.0.1:9902")
            );
            manager1 = new ClusterManager(config1WithSeeds);
            manager1.start();

            // Give time to join and re-gossip
            Thread.sleep(2500);

            // Verify Node 1 is back in membership tables and ACTIVE
            assertEquals(NodeState.ACTIVE, manager2.getMembership().get("node1").getState());
            assertEquals(NodeState.ACTIVE, manager3.getMembership().get("node1").getState());

            // Verify leadership reverted back to "node1" as it is active again
            assertEquals("node1", manager1.getCurrentLeader().getNodeId());
            assertEquals("node1", manager2.getCurrentLeader().getNodeId());
            assertEquals("node1", manager3.getCurrentLeader().getNodeId());

        } finally {
            manager1.stop();
            manager2.stop();
            manager3.stop();
        }
    }

    private void loggerInfo(String msg) {
        System.out.println("[INFO] " + msg);
    }
}
