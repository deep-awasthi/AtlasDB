package com.atlasdb.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Background garbage collector for MVCC version chains.
 * Periodically sweeps database records and prunes nodes no longer visible
 * to any active readers. Operates inside a Loom virtual thread.
 */
public final class MvccGarbageCollector implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(MvccGarbageCollector.class);

    private final HashStorageEngine<String, String> storageEngine;
    private final TimestampManager timestampManager;
    private final AtomicBoolean running = new AtomicBoolean(false);

    private Thread gcThread;

    /**
     * Constructs an MvccGarbageCollector.
     *
     * @param storageEngine    the storage engine to sweep
     * @param timestampManager the timestamp manager to query active transaction timestamps
     */
    public MvccGarbageCollector(HashStorageEngine<String, String> storageEngine, TimestampManager timestampManager) {
        this.storageEngine = storageEngine;
        this.timestampManager = timestampManager;
    }

    /**
     * Starts the garbage collection thread loop.
     */
    public synchronized void start() {
        if (running.getAndSet(true)) {
            return;
        }

        gcThread = Thread.ofVirtual()
                .name("atlasdb-mvcc-gc")
                .unstarted(this::gcLoop);
        gcThread.start();
        logger.info("MVCC Garbage Collector started.");
    }

    private void gcLoop() {
        while (running.get()) {
            try {
                Thread.sleep(1000); // Run once per second

                long minActiveTimestamp = timestampManager.getMinActiveTimestamp();
                logger.debug("Starting MVCC GC sweep. Min Active Timestamp: {}", minActiveTimestamp);

                // Collect keys to prune to avoid concurrent modification issues during iteration
                List<String> keys = new ArrayList<>();
                Iterator<Entry<String, String>> it = storageEngine.iterator();
                while (it.hasNext()) {
                    keys.add(it.next().getKey());
                }

                int prunedKeys = 0;
                int fullyDeleted = 0;

                for (String key : keys) {
                    HashStorageEngine.PruneResult result = storageEngine.pruneKey(key, minActiveTimestamp);
                    if (result.prunedVersions() > 0) {
                        prunedKeys++;
                    }
                    if (result.fullyDeleted()) {
                        fullyDeleted++;
                    }
                }

                if (prunedKeys > 0 || fullyDeleted > 0) {
                    logger.info("MVCC GC completed: pruned versions for {} keys, fully evicted {} tombstones.", 
                            prunedKeys, fullyDeleted);
                }

            } catch (InterruptedException ie) {
                break;
            } catch (Exception e) {
                logger.error("Exception in MVCC GC loop", e);
            }
        }
    }

    @Override
    public synchronized void close() {
        if (!running.getAndSet(false)) {
            return;
        }

        if (gcThread != null) {
            gcThread.interrupt();
            try {
                gcThread.join(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        logger.info("MVCC Garbage Collector stopped.");
    }
}
