package com.atlasdb.cluster;

/**
 * State of a node in the database cluster.
 */
public enum NodeState {
    /**
     * Node is bootstrapping/joining the network.
     */
    STARTING,

    /**
     * Node is active, reachable, and functioning normally.
     */
    ACTIVE,

    /**
     * Node has missed heartbeat deadlines but is not yet declared dead.
     */
    SUSPECT,

    /**
     * Node has been declared unreachable/offline.
     */
    DEAD
}
