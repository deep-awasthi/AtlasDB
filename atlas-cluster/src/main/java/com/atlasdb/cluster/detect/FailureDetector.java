package com.atlasdb.cluster.detect;

import com.atlasdb.cluster.ClusterNode;
import com.atlasdb.cluster.NodeState;
import com.atlasdb.cluster.config.ClusterConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Periodically sweeps the cluster node membership list to identify nodes
 * that have missed heartbeat deadlines, transitioning their states accordingly.
 */
public final class FailureDetector {

    private static final Logger logger = LoggerFactory.getLogger(FailureDetector.class);

    /**
     * Listener callback invoked when a node's state transitions.
     */
    public interface StateChangeListener {
        void onStateChange(ClusterNode node, NodeState oldState, NodeState newState);
    }

    private final ClusterConfig config;
    private final Map<String, ClusterNode> membership;
    private final StateChangeListener listener;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private Thread detectorThread;

    public FailureDetector(ClusterConfig config, Map<String, ClusterNode> membership, StateChangeListener listener) {
        this.config = Objects.requireNonNull(config, "config cannot be null");
        this.membership = Objects.requireNonNull(membership, "membership cannot be null");
        this.listener = Objects.requireNonNull(listener, "listener cannot be null");
    }

    /**
     * Starts the failure detection sweep thread (runs as a virtual thread).
     */
    public synchronized void start() {
        if (running.getAndSet(true)) {
            return;
        }

        detectorThread = Thread.ofVirtual().name("atlas-failure-detector").unstarted(this::runSweep);
        detectorThread.start();
        logger.info("Failure detector service started.");
    }

    /**
     * Stops the failure detector background sweeps.
     */
    public synchronized void stop() {
        if (!running.getAndSet(false)) {
            return;
        }

        if (detectorThread != null) {
            detectorThread.interrupt();
            try {
                detectorThread.join(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        logger.info("Failure detector service stopped.");
    }

    private void runSweep() {
        while (running.get()) {
            try {
                // Sleep for heartbeat interval fraction (e.g. check every 500ms)
                long sleepMs = Math.max(500, config.heartbeatIntervalMs() / 2);
                Thread.sleep(sleepMs);

                long now = System.currentTimeMillis();
                for (ClusterNode node : membership.values()) {
                    // Skip checking self
                    if (node.getNodeId().equalsIgnoreCase(config.nodeId())) {
                        continue;
                    }

                    NodeState oldState = node.getState();
                    if (oldState == NodeState.DEAD) {
                        continue;
                    }

                    long elapsed = now - node.getLastSeenTimestamp();

                    if (elapsed > config.suspectTimeoutMs()) {
                        // Mark as DEAD
                        node.setState(NodeState.DEAD);
                        logger.warn("Node '{}' declared DEAD after {}ms of silence (threshold {}ms)", 
                                node.getNodeId(), elapsed, config.suspectTimeoutMs());
                        listener.onStateChange(node, oldState, NodeState.DEAD);
                    } else if (elapsed > (config.heartbeatIntervalMs() * config.failureThresholdCount())) {
                        // Mark as SUSPECT if it was ACTIVE
                        if (oldState == NodeState.ACTIVE) {
                            node.setState(NodeState.SUSPECT);
                            logger.info("Node '{}' marked as SUSPECT after {}ms of silence", 
                                    node.getNodeId(), elapsed);
                            listener.onStateChange(node, oldState, NodeState.SUSPECT);
                        }
                    }
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                logger.error("Error during failure detector sweep execution", e);
            }
        }
    }
}
