package com.atlasdb.cluster;

import java.util.Objects;

/**
 * Metadata container representing a node in the AtlasDB cluster.
 */
public final class ClusterNode {

    private final String nodeId;
    private final String host;
    private final int port;
    private volatile NodeState state;
    private volatile long lastSeenTimestamp;

    public ClusterNode(String nodeId, String host, int port, NodeState state) {
        this.nodeId = Objects.requireNonNull(nodeId, "nodeId cannot be null");
        this.host = Objects.requireNonNull(host, "host cannot be null");
        this.port = port;
        this.state = Objects.requireNonNull(state, "state cannot be null");
        this.lastSeenTimestamp = System.currentTimeMillis();
    }

    public String getNodeId() {
        return nodeId;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public NodeState getState() {
        return state;
    }

    public void setState(NodeState state) {
        this.state = Objects.requireNonNull(state, "state cannot be null");
    }

    public long getLastSeenTimestamp() {
        return lastSeenTimestamp;
    }

    public void setLastSeenTimestamp(long lastSeenTimestamp) {
        this.lastSeenTimestamp = lastSeenTimestamp;
    }

    public void updateLastSeen() {
        this.lastSeenTimestamp = System.currentTimeMillis();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ClusterNode that = (ClusterNode) o;
        return port == that.port &&
                Objects.equals(nodeId, that.nodeId) &&
                Objects.equals(host, that.host);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodeId, host, port);
    }

    @Override
    public String toString() {
        return "ClusterNode{" +
                "nodeId='" + nodeId + '\'' +
                ", host='" + host + '\'' +
                ", port=" + port +
                ", state=" + state +
                ", lastSeen=" + lastSeenTimestamp +
                '}';
    }
}
