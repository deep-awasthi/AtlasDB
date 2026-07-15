package com.atlasdb.cluster.partition;

import com.atlasdb.cluster.ClusterManager;
import com.atlasdb.cluster.ClusterNode;
import com.atlasdb.cluster.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;

/**
 * Manages partition mappings across the cluster.
 * Synchronizes the active hash ring structure with cluster node membership transitions.
 */
public final class PartitionManager {

    private static final Logger logger = LoggerFactory.getLogger(PartitionManager.class);
    private static final int DEFAULT_VNODES = 32;

    private final ClusterManager clusterManager;
    private final int vnodeCount;
    private volatile HashRing activeRing;

    public PartitionManager(ClusterManager clusterManager) {
        this(clusterManager, DEFAULT_VNODES);
    }

    public PartitionManager(ClusterManager clusterManager, int vnodeCount) {
        this.clusterManager = Objects.requireNonNull(clusterManager, "clusterManager cannot be null");
        this.vnodeCount = vnodeCount;
        this.activeRing = new HashRing(vnodeCount);
        updateRing();
    }

    /**
     * Rebuilds the active consistent hash ring based on current membership status.
     * Excludes DEAD nodes.
     */
    public synchronized void updateRing() {
        Map<String, ClusterNode> members = clusterManager.getMembership();
        HashRing newRing = new HashRing(vnodeCount);
        
        int activeCount = 0;
        for (ClusterNode node : members.values()) {
            if (node.getState() != NodeState.DEAD) {
                newRing.addNode(node);
                activeCount++;
            }
        }
        
        this.activeRing = newRing;
        logger.debug("Consistent hash ring rebuilt. Active physical nodes: {}, virtual nodes: {}", 
                activeCount, newRing.getRingSize());
    }

    /**
     * Resolves the target node responsible for the specified key.
     *
     * @param key the database key
     * @return the cluster node owning the key
     */
    public ClusterNode getRoute(String key) {
        return activeRing.getOwner(key);
    }

    /**
     * Retrieves the current active hash ring.
     */
    public HashRing getActiveRing() {
        return activeRing;
    }
}
