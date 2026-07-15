package com.atlasdb.raft;

/**
 * Roles a Raft Node can occupy in the consensus cluster.
 */
public enum RaftRole {
    FOLLOWER,
    CANDIDATE,
    LEADER
}
