package com.atlasdb.monitoring;

import com.atlasdb.common.VersionGenerator;
import com.atlasdb.monitoring.http.MonitoringServer;
import com.atlasdb.monitoring.jmx.AtlasDbMonitor;
import com.atlasdb.monitoring.metrics.MetricsManager;
import com.atlasdb.storage.HashStorageEngine;
import com.atlasdb.storage.config.StorageConfig;
import org.junit.jupiter.api.Test;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

class MonitoringIntegrationTest {

    private int findFreePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    @Test
    void testMetricsAndHttpServerScraping() throws Exception {
        MetricsManager metrics = new MetricsManager();

        // 1. Record metrics changes
        metrics.registerDatabaseSizeGauge(() -> 15);
        metrics.incrementCommittedTransactions();
        metrics.incrementAbortedTransactions();
        metrics.setActiveTransactions(3);
        metrics.incrementRequests();
        metrics.recordRequestLatency(120);

        assertEquals(1, metrics.getCommittedTransactions());
        assertEquals(1, metrics.getAbortedTransactions());
        assertEquals(3, metrics.getActiveTransactions());
        assertEquals(1, metrics.getNetworkRequests());

        // 2. Boot HTTP server on free port
        int port = findFreePort();
        AtomicBoolean isReady = new AtomicBoolean(true);
        MonitoringServer server = new MonitoringServer(port, metrics, isReady::get);
        server.start();

        HttpClient client = HttpClient.newHttpClient();

        try {
            // Test Prometheus Scrape Endpoint (/metrics)
            HttpRequest reqMetrics = HttpRequest.newBuilder()
                    .uri(URI.create("http://localhost:" + port + "/metrics"))
                    .GET()
                    .build();
            HttpResponse<String> respMetrics = client.send(reqMetrics, HttpResponse.BodyHandlers.ofString());

            assertEquals(200, respMetrics.statusCode());
            assertTrue(respMetrics.headers().firstValue("Content-Type").isPresent());
            assertTrue(respMetrics.headers().firstValue("Content-Type").get().contains("text/plain"));
            
            String metricsBody = respMetrics.body();
            assertTrue(metricsBody.contains("atlasdb_transactions_committed_total"));
            assertTrue(metricsBody.contains("atlasdb_transactions_active"));
            assertTrue(metricsBody.contains("atlasdb_database_size 15"));

            // Test Liveness Endpoint (/health/liveness)
            HttpRequest reqLiveness = HttpRequest.newBuilder()
                    .uri(URI.create("http://localhost:" + port + "/health/liveness"))
                    .GET()
                    .build();
            HttpResponse<String> respLiveness = client.send(reqLiveness, HttpResponse.BodyHandlers.ofString());

            assertEquals(200, respLiveness.statusCode());
            assertEquals("UP", respLiveness.body());

            // Test Readiness Endpoint - Healthy (/health/readiness)
            HttpRequest reqReadiness = HttpRequest.newBuilder()
                    .uri(URI.create("http://localhost:" + port + "/health/readiness"))
                    .GET()
                    .build();
            HttpResponse<String> respReadinessHealthy = client.send(reqReadiness, HttpResponse.BodyHandlers.ofString());

            assertEquals(200, respReadinessHealthy.statusCode());
            assertEquals("READY", respReadinessHealthy.body());

            // Test Readiness Endpoint - Unhealthy
            isReady.set(false);
            HttpResponse<String> respReadinessUnhealthy = client.send(reqReadiness, HttpResponse.BodyHandlers.ofString());

            assertEquals(503, respReadinessUnhealthy.statusCode());
            assertEquals("DOWN", respReadinessUnhealthy.body());

        } finally {
            server.stop();
        }
    }

    @Test
    void testJmxMBeanManagement() throws Exception {
        // 1. Setup Engine and Monitor MBean
        VersionGenerator vg = new VersionGenerator();
        HashStorageEngine<String, String> engine = new HashStorageEngine<>(new StorageConfig(4, 0.75f), vg);
        
        MetricsManager metrics = new MetricsManager();
        metrics.setActiveTransactions(5);

        engine.put("k1", "v1", 100);
        engine.put("k2", "v2", 200);

        AtlasDbMonitor monitor = new AtlasDbMonitor(engine, metrics);

        // 2. Register MBean
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        ObjectName name = new ObjectName("com.atlasdb:type=AtlasDbMonitorTests");

        if (mbs.isRegistered(name)) {
            mbs.unregisterMBean(name);
        }

        mbs.registerMBean(monitor, name);

        try {
            // 3. Query JMX Attributes
            int dbSize = (int) mbs.getAttribute(name, "DatabaseSize");
            assertEquals(2, dbSize);

            long activeTx = (long) mbs.getAttribute(name, "ActiveTransactions");
            assertEquals(5, activeTx);

            // 4. Invoke GC operation
            mbs.invoke(name, "triggerGarbageCollection", null, null);

        } finally {
            // Unregister MBean to avoid test leaks
            mbs.unregisterMBean(name);
        }
    }
}
