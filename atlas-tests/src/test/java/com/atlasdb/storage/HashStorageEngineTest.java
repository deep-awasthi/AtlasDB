package com.atlasdb.storage;

import com.atlasdb.common.VersionGenerator;
import com.atlasdb.storage.config.StorageConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

class HashStorageEngineTest {

    private HashStorageEngine<String, String> engine;
    private VersionGenerator versionGenerator;

    @BeforeEach
    void setUp() {
        versionGenerator = new VersionGenerator();
        StorageConfig config = new StorageConfig(4, 0.75f); // Small capacity to force resizing easily
        engine = new HashStorageEngine<>(config, versionGenerator);
    }

    @Test
    void testBasicPutAndGet() {
        long v1 = engine.put("k1", "v1");
        long v2 = engine.put("k2", "v2");

        assertTrue(v1 > 0);
        assertTrue(v2 > v1);
        assertEquals("v1", engine.get("k1"));
        assertEquals("v2", engine.get("k2"));
        assertEquals(2, engine.size());

        // Update
        long v3 = engine.put("k1", "v1-updated");
        assertTrue(v3 > v2);
        assertEquals("v1-updated", engine.get("k1"));
        assertEquals(2, engine.size());
    }

    @Test
    void testGetNonExistentKey() {
        assertNull(engine.get("non-existent"));
        assertEquals(-1L, engine.getVersion("non-existent"));
        assertFalse(engine.containsKey("non-existent"));
    }

    @Test
    void testNullKeysAndValues() {
        assertThrows(IllegalArgumentException.class, () -> engine.put(null, "val"));
        assertThrows(IllegalArgumentException.class, () -> engine.put("key", null));
        assertThrows(IllegalArgumentException.class, () -> engine.get(null));
        assertThrows(IllegalArgumentException.class, () -> engine.delete(null));
    }

    @Test
    void testDelete() {
        engine.put("k1", "v1");
        assertTrue(engine.containsKey("k1"));

        assertTrue(engine.delete("k1"));
        assertFalse(engine.containsKey("k1"));
        assertNull(engine.get("k1"));
        assertEquals(0, engine.size());

        // Delete non-existent
        assertFalse(engine.delete("k1"));
    }

    @Test
    void testDynamicResizing() {
        // Initial capacity is 4. Load factor is 0.75. Resize triggers at size > 3.
        engine.put("k1", "v1");
        engine.put("k2", "v2");
        engine.put("k3", "v3");

        MemoryStats statsBefore = engine.getMemoryStats();
        assertEquals(4, statsBefore.bucketCount());

        // 4th element triggers resize to capacity 8
        engine.put("k4", "v4");

        MemoryStats statsAfter = engine.getMemoryStats();
        assertEquals(8, statsAfter.bucketCount());
        assertEquals(4, engine.size());

        // Verify elements are still accessible
        assertEquals("v1", engine.get("k1"));
        assertEquals("v2", engine.get("k2"));
        assertEquals("v3", engine.get("k3"));
        assertEquals("v4", engine.get("k4"));
    }

    @Test
    void testMemoryStatistics() {
        engine.put("k1", "v1");
        MemoryStats stats = engine.getMemoryStats();

        assertTrue(stats.entryCount() > 0);
        assertTrue(stats.keyBytes() > 0);
        assertTrue(stats.valueBytes() > 0);
        assertTrue(stats.overheadBytes() > 0);
        assertEquals(stats.totalBytes(), stats.keyBytes() + stats.valueBytes() + stats.overheadBytes());

        long prevTotal = stats.totalBytes();

        // Update with larger value
        engine.put("k1", "v1-much-larger-value");
        MemoryStats statsUpdated = engine.getMemoryStats();
        assertTrue(statsUpdated.valueBytes() > stats.valueBytes());
        assertTrue(statsUpdated.totalBytes() > prevTotal);

        // Delete
        engine.delete("k1");
        engine.pruneKey("k1", versionGenerator.currentVersion());
        MemoryStats statsDeleted = engine.getMemoryStats();
        assertEquals(0, statsDeleted.entryCount());
        assertEquals(0, statsDeleted.keyBytes());
        assertEquals(0, statsDeleted.valueBytes());
    }

    @Test
    void testIteratorWeakConsistency() {
        engine.put("k1", "v1");
        engine.put("k2", "v2");
        engine.put("k3", "v3");

        Iterator<Entry<String, String>> iterator = engine.iterator();
        assertTrue(iterator.hasNext());

        // Concurrently modify while iterating
        engine.put("k4", "v4");
        engine.delete("k2");

        List<String> iteratedKeys = new ArrayList<>();
        while (iterator.hasNext()) {
            iteratedKeys.add(iterator.next().getKey());
        }

        // Iterator must run without throwing ConcurrentModificationException.
        // It should contain "k1", "k3" and maybe "k4" / "k2" depending on placement, but definitely no crash.
        assertTrue(iteratedKeys.contains("k1"));
        assertTrue(iteratedKeys.contains("k3"));
    }

    @Test
    void testClear() {
        engine.put("k1", "v1");
        engine.put("k2", "v2");
        assertEquals(2, engine.size());

        engine.clear();
        assertEquals(0, engine.size());
        assertNull(engine.get("k1"));
        assertNull(engine.get("k2"));

        MemoryStats stats = engine.getMemoryStats();
        assertEquals(0, stats.entryCount());
        assertEquals(0, stats.keyBytes());
        assertEquals(0, stats.valueBytes());
    }

    @Test
    void testConcurrentReadWriteResizing() throws Exception {
        int threads = 16;
        int operationsPerThread = 1000;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch latch = new CountDownLatch(1);

        List<Future<Void>> futures = new ArrayList<>();

        for (int t = 0; t < threads; t++) {
            final int threadId = t;
            futures.add(executor.submit(() -> {
                latch.await();
                for (int i = 0; i < operationsPerThread; i++) {
                    String key = "key-" + threadId + "-" + i;
                    String val = "value-" + i;
                    
                    // Put
                    engine.put(key, val);
                    
                    // Get
                    assertEquals(val, engine.get(key));
                    
                    // Conditionally delete some keys to generate mixed load
                    if (i % 5 == 0) {
                        assertTrue(engine.delete(key));
                        assertNull(engine.get(key));
                    }
                }
                return null;
            }));
        }

        // Start all threads simultaneously
        latch.countDown();

        for (Future<Void> future : futures) {
            future.get(10, TimeUnit.SECONDS); // Block until done
        }

        executor.shutdown();
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));

        // The size should match the non-deleted elements
        // Total elements written = 16 * 1000 = 16000
        // Deleted elements = 16 * (1000 / 5) = 3200
        // Expected size = 12800
        assertEquals(12800, engine.size());
        
        // Check that resizing happened dynamically
        assertTrue(engine.getMemoryStats().bucketCount() > 4);
    }
}
