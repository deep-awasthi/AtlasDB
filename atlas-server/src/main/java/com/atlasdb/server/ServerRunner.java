package com.atlasdb.server;

import com.atlasdb.cluster.ClusterManager;
import com.atlasdb.cluster.config.ClusterConfig;
import com.atlasdb.common.VersionGenerator;
import com.atlasdb.monitoring.http.MonitoringServer;
import com.atlasdb.monitoring.metrics.MetricsManager;
import com.atlasdb.network.server.TcpServer;
import com.atlasdb.raft.RaftNode;
import com.atlasdb.storage.HashStorageEngine;
import com.atlasdb.storage.config.StorageConfig;
import com.atlasdb.storage.persistence.WriteAheadLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Main application bootloader that configures and launches a standalone distributed database node.
 */
public final class ServerRunner {

    private static final Logger logger = LoggerFactory.getLogger(ServerRunner.class);

    public static void main(String[] args) {
        logger.info("Initializing AtlasDB Server Node...");

        // 1. Resolve environment configurations
        String nodeId = getEnv("NODE_ID", "node1");
        String host = getEnv("HOST", "127.0.0.1");
        int clientPort = getEnvInt("CLIENT_PORT", 8601);
        int clusterPort = getEnvInt("CLUSTER_PORT", 9601);
        int monitoringPort = getEnvInt("MONITORING_PORT", 7601);
        String dataDirStr = getEnv("DATA_DIR", "data/" + nodeId);
        String dbMode = getEnv("DB_MODE", "HYBRID");
        
        String seedsStr = getEnv("SEEDS", "");
        List<String> seeds = new ArrayList<>();
        if (!seedsStr.isBlank()) {
            seeds = Arrays.stream(seedsStr.split(","))
                    .map(String::trim)
                    .filter(s -> !s.isEmpty())
                    .collect(Collectors.toList());
        }

        File dataDir = new File(dataDirStr);

        logger.info("Configuration Loaded:");
        logger.info("  Node ID:         {}", nodeId);
        logger.info("  Host Address:    {}", host);
        logger.info("  Client Port:     {}", clientPort);
        logger.info("  Cluster Port:    {}", clusterPort);
        logger.info("  Monitoring Port: {}", monitoringPort);
        logger.info("  Database Mode:   {}", dbMode);
        logger.info("  Data Directory:  {}", dataDir.getAbsolutePath());
        logger.info("  Cluster Seeds:   {}", seeds);

        try {
            // 2. Storage System
            VersionGenerator versionGenerator = new VersionGenerator();
            HashStorageEngine<String, String> storageEngine = new HashStorageEngine<>(
                    new StorageConfig(16, 0.75f, dbMode),
                    versionGenerator
            );

            // Register WAL
            File walDir = new File(dataDir, "wal");
            WriteAheadLog wal = new WriteAheadLog(walDir, true);
            storageEngine.registerWal(wal);

            // Replay existing logs on startup
            logger.info("Replaying Write-Ahead Logs...");
            storageEngine.clear();
            wal.readAll().forEach(entry -> {
                if (entry.getType() == com.atlasdb.storage.persistence.WalEntry.TYPE_DELETE) {
                    storageEngine.delete(entry.getKey(), entry.getVersion());
                } else {
                    storageEngine.put(entry.getKey(), entry.getValue(), entry.getVersion());
                }
            });
            logger.info("WAL replay complete. Database size: {} records.", storageEngine.size());

            // 3. Client Networking Server
            TcpServer clientServer = new TcpServer(host, clientPort, storageEngine);
            clientServer.start();

            // Register SQL execution handler using transaction manager
            com.atlasdb.storage.TimestampManager timestampManager = new com.atlasdb.storage.TimestampManager(versionGenerator);
            com.atlasdb.transaction.TransactionManager transactionManager = new com.atlasdb.transaction.TransactionManager(storageEngine, timestampManager);

            clientServer.registerSqlQueryHandler(sql -> {
                com.atlasdb.transaction.Transaction txn = transactionManager.begin(
                        com.atlasdb.transaction.Transaction.IsolationLevel.SNAPSHOT_ISOLATION,
                        com.atlasdb.transaction.Transaction.ConcurrencyMode.OPTIMISTIC
                );
                try {
                    com.atlasdb.sql.catalog.CatalogManager catalog = new com.atlasdb.sql.catalog.CatalogManager();
                    com.atlasdb.sql.executor.SqlExecutor executor = new com.atlasdb.sql.executor.SqlExecutor(catalog);
                    com.atlasdb.sql.executor.ResultSet rs = executor.execute(txn, sql);
                    txn.commit();
                    return rs.toString();
                } catch (Exception e) {
                    txn.abort();
                    throw e;
                }
            });

            // 4. Cluster Manager
            ClusterConfig clusterConfig = new ClusterConfig(
                    nodeId,
                    host,
                    clusterPort,
                    1000,
                    3,
                    5000,
                    seeds
            );
            ClusterManager clusterManager = new ClusterManager(clusterConfig);
            clusterManager.start();

            // 5. Consensus Engine (Raft Node - 1500ms timeout)
            RaftNode raftNode = new RaftNode(clusterConfig, storageEngine, clusterManager, clientServer, 1500);
            raftNode.start();

            // 6. Monitoring & HTTP Metrics Export
            MetricsManager metricsManager = new MetricsManager();
            metricsManager.registerDatabaseSizeGauge(storageEngine::size);
            
            MonitoringServer monitoringServer = new MonitoringServer(
                    monitoringPort,
                    metricsManager,
                    () -> !clusterManager.getMembership().isEmpty() // Simple readiness check
            );
            monitoringServer.start();

            logger.info("AtlasDB Server Node '{}' booted successfully and online.", nodeId);

            // 7. Grateful Shutdown Hook
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.info("Shutdown signal received. stopping node resources gracefully...");
                monitoringServer.stop();
                raftNode.stop();
                clusterManager.stop();
                clientServer.stop();
                try {
                    wal.close();
                } catch (Exception e) {
                    logger.error("Failed to close WAL: {}", e.getMessage());
                }
                logger.info("AtlasDB Node shutdown completed.");
            }, "atlasdb-shutdown-hook"));

            // Keep the main thread alive waiting
            Thread.currentThread().join();

        } catch (Exception e) {
            logger.error("FATAL: Failed to boot AtlasDB Node: {}", e.getMessage(), e);
            System.exit(1);
        }
    }

    private static String getEnv(String var, String defaultVal) {
        String val = System.getenv(var);
        return (val == null) ? defaultVal : val;
    }

    private static int getEnvInt(String var, int defaultVal) {
        String val = System.getenv(var);
        if (val == null) {
            return defaultVal;
        }
        try {
            return Integer.parseInt(val);
        } catch (NumberFormatException e) {
            logger.warn("Invalid integer environment config for '{}': '{}'. Using default: {}", var, val, defaultVal);
            return defaultVal;
        }
    }
}
