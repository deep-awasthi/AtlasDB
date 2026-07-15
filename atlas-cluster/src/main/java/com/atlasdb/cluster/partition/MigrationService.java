package com.atlasdb.cluster.partition;

import com.atlasdb.cluster.ClusterNode;
import com.atlasdb.cluster.config.ClusterConfig;
import com.atlasdb.network.client.TcpClient;
import com.atlasdb.network.protocol.Packet;
import com.atlasdb.storage.StorageEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Executes out-of-range database key migrations to their new consistent hashing owner nodes.
 */
public final class MigrationService {

    private static final Logger logger = LoggerFactory.getLogger(MigrationService.class);

    private final ClusterConfig config;
    private final StorageEngine<String, String> storageEngine;
    private final PartitionManager partitionManager;
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
    private final AtomicBoolean migrating = new AtomicBoolean(false);

    public MigrationService(ClusterConfig config, StorageEngine<String, String> storageEngine, PartitionManager partitionManager) {
        this.config = Objects.requireNonNull(config, "config cannot be null");
        this.storageEngine = Objects.requireNonNull(storageEngine, "storageEngine cannot be null");
        this.partitionManager = Objects.requireNonNull(partitionManager, "partitionManager cannot be null");
    }

    /**
     * Triggers the rebalancing migration scan in a background virtual thread.
     */
    public void triggerRebalance() {
        if (migrating.getAndSet(true)) {
            logger.debug("Migration already in progress. Skipping trigger.");
            return;
        }

        executor.submit(this::runMigrationScan);
    }

    /**
     * Shuts down the migration executor service.
     */
    public void shutdown() {
        executor.shutdownNow();
    }

    private void runMigrationScan() {
        logger.info("Starting database sharding migration sweep on Node '{}'...", config.nodeId());
        int migratedCount = 0;

        try {
            Iterator<?> it = storageEngine.iterator();
            while (it.hasNext()) {
                com.atlasdb.storage.Entry<?, ?> entry = (com.atlasdb.storage.Entry<?, ?>) it.next();
                String key = (String) entry.getKey();
                
                // Exclude system catalog metadata keys from shard migration
                if (key.startsWith("catalog:")) {
                    continue;
                }

                ClusterNode targetOwner = partitionManager.getRoute(key);
                if (targetOwner != null && !targetOwner.getNodeId().equalsIgnoreCase(config.nodeId())) {
                    String value = storageEngine.get(key);
                    if (value != null) {
                        boolean success = transmitKeyToNode(targetOwner, key, value);
                        if (success) {
                            storageEngine.delete(key);
                            migratedCount++;
                            logger.debug("Key '{}' migrated from '{}' to '{}'", key, config.nodeId(), targetOwner.getNodeId());
                        }
                    }
                }
            }
            logger.info("Sharding migration sweep finished. Total keys migrated: {}", migratedCount);
        } catch (Exception e) {
            logger.error("Error during sharding migration sweep", e);
        } finally {
            migrating.set(false);
        }
    }

    private boolean transmitKeyToNode(ClusterNode target, String key, String value) {
        // Predictable client port mapping from cluster port
        int targetClientPort = target.getPort() - 1000;

        try (TcpClient client = new TcpClient(target.getHost(), targetClientPort, 3000)) {
            client.connect();

            byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
            byte[] valBytes = value.getBytes(StandardCharsets.UTF_8);

            ByteBuffer payload = ByteBuffer.allocate(4 + keyBytes.length + 4 + valBytes.length);
            payload.putInt(keyBytes.length);
            payload.put(keyBytes);
            payload.putInt(valBytes.length);
            payload.put(valBytes);
            payload.flip();

            Packet response = client.send(new Packet(Packet.TYPE_REQ_PUT, 12345, 0, payload.array()));
            return response.getType() == Packet.TYPE_RESP_SUCCESS;
        } catch (Exception e) {
            logger.error("Failed to migrate key '{}' to target Node '{}' ({}:{}): {}", 
                    key, target.getNodeId(), target.getHost(), targetClientPort, e.getMessage());
            return false;
        }
    }
}
