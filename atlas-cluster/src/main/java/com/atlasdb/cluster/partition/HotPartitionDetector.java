package com.atlasdb.cluster.partition;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Monitors and identifies hotspots (high read/write throughput key ranges)
 * in the database partition layout.
 */
public final class HotPartitionDetector {

    private static final Logger logger = LoggerFactory.getLogger(HotPartitionDetector.class);

    private final Map<String, Long> accessCounts = new HashMap<>();
    private final ReentrantLock counterLock = new ReentrantLock();

    private final long hotThresholdPerSec;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private Thread monitorThread;

    public HotPartitionDetector(long hotThresholdPerSec) {
        this.hotThresholdPerSec = hotThresholdPerSec;
    }

    /**
     * Starts the periodic metrics sweep thread.
     */
    public synchronized void start() {
        if (running.getAndSet(true)) {
            return;
        }

        monitorThread = Thread.ofVirtual().name("atlas-hotspot-detector").unstarted(this::runMonitor);
        monitorThread.start();
        logger.info("Hot Partition Detector started with threshold: {} req/sec", hotThresholdPerSec);
    }

    /**
     * Stops the metrics sweep thread.
     */
    public synchronized void stop() {
        if (!running.getAndSet(false)) {
            return;
        }

        if (monitorThread != null) {
            monitorThread.interrupt();
            try {
                monitorThread.join(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        logger.info("Hot Partition Detector stopped.");
    }

    /**
     * Records a read or write access event on a key.
     * Extracts prefix (e.g. "table:users" from "table:users:1") to group counts.
     */
    public void recordAccess(String key) {
        if (key == null) {
            return;
        }

        String prefix = extractPrefix(key);
        counterLock.lock();
        try {
            accessCounts.put(prefix, accessCounts.getOrDefault(prefix, 0L) + 1);
        } finally {
            counterLock.unlock();
        }
    }

    private String extractPrefix(String key) {
        int firstColon = key.indexOf(':');
        if (firstColon == -1) {
            return key;
        }
        int secondColon = key.indexOf(':', firstColon + 1);
        if (secondColon == -1) {
            return key.substring(0, firstColon);
        }
        return key.substring(0, secondColon);
    }

    private void runMonitor() {
        while (running.get()) {
            try {
                Thread.sleep(1000);

                Map<String, Long> snapshot;
                counterLock.lock();
                try {
                    snapshot = new HashMap<>(accessCounts);
                    accessCounts.clear(); // reset counters for next window
                } finally {
                    counterLock.unlock();
                }

                for (Map.Entry<String, Long> entry : snapshot.entrySet()) {
                    long rate = entry.getValue();
                    if (rate > hotThresholdPerSec) {
                        logger.warn("HOT PARTITION DETECTED - Range prefix '{}' had {} accesses/sec (threshold {})", 
                                entry.getKey(), rate, hotThresholdPerSec);
                    }
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                logger.error("Error in hot partition detector monitoring execution", e);
            }
        }
    }
}
