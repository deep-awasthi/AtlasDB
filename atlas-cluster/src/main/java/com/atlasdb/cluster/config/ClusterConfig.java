package com.atlasdb.cluster.config;

import java.util.List;
import java.util.Objects;

/**
 * Configuration parameters for the clustering and membership layers.
 */
public record ClusterConfig(
        String nodeId,
        String host,
        int port,
        long heartbeatIntervalMs,
        int failureThresholdCount,
        long suspectTimeoutMs,
        List<String> seedNodes
) {
    public ClusterConfig {
        Objects.requireNonNull(nodeId, "nodeId cannot be null");
        Objects.requireNonNull(host, "host cannot be null");
        Objects.requireNonNull(seedNodes, "seedNodes cannot be null");
        if (port <= 0 || port > 65535) {
            throw new IllegalArgumentException("Invalid port: " + port);
        }
        if (heartbeatIntervalMs <= 0) {
            throw new IllegalArgumentException("heartbeatIntervalMs must be positive");
        }
        if (failureThresholdCount <= 0) {
            throw new IllegalArgumentException("failureThresholdCount must be positive");
        }
        if (suspectTimeoutMs <= 0) {
            throw new IllegalArgumentException("suspectTimeoutMs must be positive");
        }
    }
}
