package com.atlasdb.cluster.lead;

import com.atlasdb.cluster.ClusterNode;
import com.atlasdb.cluster.NodeState;

import java.util.Comparator;
import java.util.Map;
import java.util.Objects;

/**
 * Deterministically resolves the current cluster coordinator (leader) based on active memberships.
 * The active node with the lowest lexicographical nodeId is selected.
 */
public final class LeaderSelector {

    /**
     * Identifies the current leader node.
     *
     * @param membership the current membership table
     * @return the leader Node, or null if no active nodes are available
     */
    public ClusterNode selectLeader(Map<String, ClusterNode> membership) {
        if (membership == null || membership.isEmpty()) {
            return null;
        }

        return membership.values().stream()
                .filter(node -> node.getState() == NodeState.ACTIVE || node.getState() == NodeState.STARTING)
                .min(Comparator.comparing(ClusterNode::getNodeId))
                .orElse(null);
    }

    /**
     * Checks if the local node is currently resolved as the leader.
     *
     * @param localNodeId the local node ID
     * @param membership  the current membership table
     * @return true if the local node is the coordinator
     */
    public boolean isLocalNodeLeader(String localNodeId, Map<String, ClusterNode> membership) {
        Objects.requireNonNull(localNodeId, "localNodeId cannot be null");
        ClusterNode leader = selectLeader(membership);
        return leader != null && leader.getNodeId().equalsIgnoreCase(localNodeId);
    }
}
