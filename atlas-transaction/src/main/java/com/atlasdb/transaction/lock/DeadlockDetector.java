package com.atlasdb.transaction.lock;

import com.atlasdb.transaction.TransactionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Background deadlock detector. Periodically requests wait-for graphs from
 * LockManager, performs DFS cycle detection, and aborts conflicting transactions.
 * Runs in a background Loom virtual thread.
 */
public final class DeadlockDetector implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(DeadlockDetector.class);

    private final LockManager lockManager;
    private final TransactionManager transactionManager;
    private final AtomicBoolean running = new AtomicBoolean(false);
    
    private Thread detectorThread;
    private final long detectionIntervalMs;

    /**
     * Constructs a DeadlockDetector.
     *
     * @param lockManager        the lock manager
     * @param transactionManager the transaction manager to abort deadlocked transactions
     * @param intervalMs         checking interval in milliseconds
     */
    public DeadlockDetector(LockManager lockManager, TransactionManager transactionManager, long intervalMs) {
        this.lockManager = lockManager;
        this.transactionManager = transactionManager;
        this.detectionIntervalMs = intervalMs;
    }

    /**
     * Starts the deadlock detection thread loop.
     */
    public synchronized void start() {
        if (running.getAndSet(true)) {
            return;
        }

        detectorThread = Thread.ofVirtual()
                .name("atlasdb-deadlock-detector")
                .unstarted(this::detectLoop);
        detectorThread.start();
        logger.info("Deadlock Detector background thread started.");
    }

    private void detectLoop() {
        while (running.get()) {
            try {
                Thread.sleep(detectionIntervalMs);
                
                Map<Long, Set<Long>> graph = lockManager.getWaitForGraph();
                if (graph.isEmpty()) {
                    continue;
                }

                List<Long> cycle = findCycle(graph);
                if (!cycle.isEmpty()) {
                    logger.warn("Deadlock detected! Cycle: {}", cycle);
                    
                    // Select victim: youngest transaction (highest Txn ID)
                    long victim = selectVictim(cycle);
                    logger.warn("Aborting transaction {} to resolve deadlock", victim);
                    
                    transactionManager.abort(victim);
                }
            } catch (InterruptedException e) {
                break;
            } catch (Exception e) {
                logger.error("Exception in Deadlock Detector loop", e);
            }
        }
    }

    /**
     * Scans wait-for graph for cycles using Depth-First Search.
     *
     * @param graph wait-for graph mapping TxnId -> Set of TxnId it is waiting for
     * @return list of transaction IDs involved in the cycle, or empty list if no cycle
     */
    private List<Long> findCycle(Map<Long, Set<Long>> graph) {
        Set<Long> visited = new HashSet<>();
        Set<Long> stack = new LinkedHashSet<>(); // Keeps insertion order to easily extract cycle path

        for (Long node : graph.keySet()) {
            if (!visited.contains(node)) {
                List<Long> cycle = dfs(node, graph, visited, stack);
                if (!cycle.isEmpty()) {
                    return cycle;
                }
            }
        }
        return Collections.emptyList();
    }

    private List<Long> dfs(Long node, Map<Long, Set<Long>> graph, Set<Long> visited, Set<Long> stack) {
        visited.add(node);
        stack.add(node);

        Set<Long> neighbors = graph.get(node);
        if (neighbors != null) {
            for (Long neighbor : neighbors) {
                if (stack.contains(neighbor)) {
                    // Cycle detected! Extract path from neighbor to node
                    List<Long> cycle = new ArrayList<>();
                    boolean startAdding = false;
                    for (Long s : stack) {
                        if (s.equals(neighbor)) {
                            startAdding = true;
                        }
                        if (startAdding) {
                            cycle.add(s);
                        }
                    }
                    return cycle;
                }
                
                if (!visited.contains(neighbor)) {
                    List<Long> cycle = dfs(neighbor, graph, visited, stack);
                    if (!cycle.isEmpty()) {
                        return cycle;
                    }
                }
            }
        }

        stack.remove(node);
        return Collections.emptyList();
    }

    private long selectVictim(List<Long> cycle) {
        // Victim selection: youngest transaction (highest ID)
        long victim = -1;
        for (long txnId : cycle) {
            if (txnId > victim) {
                victim = txnId;
            }
        }
        return victim;
    }

    @Override
    public synchronized void close() {
        if (!running.getAndSet(false)) {
            return;
        }

        if (detectorThread != null) {
            detectorThread.interrupt();
            try {
                detectorThread.join(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        logger.info("Deadlock Detector background thread stopped.");
    }
}
