package com.atlasdb.storage;

import com.atlasdb.common.VersionGenerator;
import com.atlasdb.storage.config.StorageConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class MvccIntegrationTest {

    private HashStorageEngine<String, String> engine;
    private VersionGenerator versionGenerator;
    private TimestampManager timestampManager;
    private MvccGarbageCollector garbageCollector;

    @BeforeEach
    void setUp() {
        versionGenerator = new VersionGenerator();
        engine = new HashStorageEngine<>(
                new StorageConfig(8, 0.75f),
                versionGenerator
        );
        timestampManager = new TimestampManager(versionGenerator);
        garbageCollector = new MvccGarbageCollector(engine, timestampManager);
        
        // Start background GC
        garbageCollector.start();
    }

    @AfterEach
    void tearDown() {
        if (garbageCollector != null) {
            garbageCollector.close();
        }
    }

    @Test
    void testSnapshotReadsAndMultiVersionChains() {
        // Write v1
        long tx1 = engine.put("k1", "v1");
        
        // Write v2
        long tx2 = engine.put("k1", "v2");

        // Write v3
        long tx3 = engine.put("k1", "v3");

        // Verify snapshot reads see correct values at respective points in time
        assertEquals("v1", engine.get("k1", tx1));
        assertEquals("v2", engine.get("k1", tx2));
        assertEquals("v3", engine.get("k1", tx3));
        
        // Before tx1, key should not exist
        assertNull(engine.get("k1", tx1 - 1));
    }

    @Test
    void testActiveReaderPreventsGarbageCollection() throws InterruptedException {
        long ts1 = engine.put("k1", "v1");
        long ts2 = engine.put("k1", "v2");
        long ts3 = engine.put("k1", "v3");

        // Register a reader at ts1
        timestampManager.registerReader(ts1);

        // Run GC (manually or wait for background. Manually call engine's prune to be instant)
        long minTs = timestampManager.getMinActiveTimestamp();
        assertEquals(ts1, minTs); // Minimum active is ts1

        HashStorageEngine.PruneResult result = engine.pruneKey("k1", minTs);
        // Pruning should NOT discard v1 because the active reader has snapshot at ts1.
        // It cannot discard anything because ts1 is the oldest active read.
        assertEquals(0, result.prunedVersions());

        // Verify k1 versions are still visible
        assertEquals("v1", engine.get("k1", ts1));
        assertEquals("v2", engine.get("k1", ts2));

        // Deregister reader and register new reader at ts2
        timestampManager.deregisterReader(ts1);
        timestampManager.registerReader(ts2);

        minTs = timestampManager.getMinActiveTimestamp();
        assertEquals(ts2, minTs);

        result = engine.pruneKey("k1", minTs);
        // We can prune versions older than ts2 (i.e. ts1 is pruned)
        // because the oldest active reader is at ts2.
        assertEquals(1, result.prunedVersions());

        // ts1 should now return null because it's been pruned
        assertNull(engine.get("k1", ts1));
        assertEquals("v2", engine.get("k1", ts2));
        assertEquals("v3", engine.get("k1", ts3));

        // Cleanup
        timestampManager.deregisterReader(ts2);
    }

    @Test
    void testTombstoneGarbageCollection() {
        long ts1 = engine.put("k1", "v1");
        engine.delete("k1"); // Creates a tombstone
        long ts2 = versionGenerator.currentVersion();

        assertNull(engine.get("k1"));

        // With no active readers, min active is current
        long minTs = timestampManager.getMinActiveTimestamp();
        assertTrue(minTs >= ts2);

        HashStorageEngine.PruneResult result = engine.pruneKey("k1", minTs);
        // Since k1 is deleted (tombstone is head) and no readers can see ts1 or ts2,
        // it should be fully evicted from the engine
        assertTrue(result.fullyDeleted());
        assertEquals(0, engine.size());
        assertNull(engine.get("k1"));
    }

    @Test
    void testMemoryStatisticsPruning() {
        engine.put("k1", "v1");
        engine.put("k1", "v2");
        engine.put("k1", "v3");

        MemoryStats statsBefore = engine.getMemoryStats();

        // Perform GC with no active readers
        long minTs = timestampManager.getMinActiveTimestamp();
        engine.pruneKey("k1", minTs);

        MemoryStats statsAfter = engine.getMemoryStats();

        // Memory should have decreased as v1 and v2 nodes are discarded
        assertTrue(statsAfter.valueBytes() < statsBefore.valueBytes());
        assertTrue(statsAfter.overheadBytes() < statsBefore.overheadBytes());
        assertTrue(statsAfter.totalBytes() < statsBefore.totalBytes());
    }
}
