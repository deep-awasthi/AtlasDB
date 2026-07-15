package com.atlasdb.monitoring.http;

import com.atlasdb.monitoring.metrics.MetricsManager;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Embedded HTTP server hosting Prometheus metric scrapes and Health endpoints.
 */
public final class MonitoringServer {

    private static final Logger logger = LoggerFactory.getLogger(MonitoringServer.class);

    private final int port;
    private final MetricsManager metricsManager;
    private final Supplier<Boolean> readinessCheck;
    private HttpServer server;

    public MonitoringServer(int port, MetricsManager metricsManager, Supplier<Boolean> readinessCheck) {
        this.port = port;
        this.metricsManager = Objects.requireNonNull(metricsManager, "metricsManager cannot be null");
        this.readinessCheck = Objects.requireNonNull(readinessCheck, "readinessCheck cannot be null");
    }

    /**
     * Starts the embedded JRE HTTP server.
     */
    public synchronized void start() throws IOException {
        if (server != null) {
            return;
        }

        server = HttpServer.create(new InetSocketAddress("localhost", port), 0);
        
        server.createContext("/metrics", new MetricsHandler());
        server.createContext("/health/liveness", new LivenessHandler());
        server.createContext("/health/readiness", new ReadinessHandler());

        server.setExecutor(null); // default executor
        server.start();
        logger.info("Monitoring HTTP Server started on http://localhost:{}", port);
    }

    /**
     * Stops the HTTP server.
     */
    public synchronized void stop() {
        if (server != null) {
            server.stop(0);
            server = null;
            logger.info("Monitoring HTTP Server stopped on port {}", port);
        }
    }

    private class MetricsHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(405, -1);
                return;
            }

            String response = metricsManager.getRegistry().scrape();
            byte[] bytes = response.getBytes(StandardCharsets.UTF_8);

            exchange.getResponseHeaders().set("Content-Type", "text/plain; version=0.0.4; charset=utf-8");
            exchange.sendResponseHeaders(200, bytes.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(bytes);
            }
        }
    }

    private static class LivenessHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(405, -1);
                return;
            }

            String response = "UP";
            byte[] bytes = response.getBytes(StandardCharsets.UTF_8);

            exchange.sendResponseHeaders(200, bytes.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(bytes);
            }
        }
    }

    private class ReadinessHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(405, -1);
                return;
            }

            boolean ready = false;
            try {
                ready = readinessCheck.get();
            } catch (Exception e) {
                logger.error("Readiness check error: {}", e.getMessage());
            }

            String response = ready ? "READY" : "DOWN";
            byte[] bytes = response.getBytes(StandardCharsets.UTF_8);
            int statusCode = ready ? 200 : 503;

            exchange.sendResponseHeaders(statusCode, bytes.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(bytes);
            }
        }
    }
}
