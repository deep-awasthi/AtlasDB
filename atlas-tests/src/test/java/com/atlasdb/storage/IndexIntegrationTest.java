package com.atlasdb.storage;

import com.atlasdb.common.VersionGenerator;
import com.atlasdb.storage.config.StorageConfig;
import com.atlasdb.storage.index.IndexManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class IndexIntegrationTest {

    private HashStorageEngine<String, String> engine;
    private IndexManager indexManager;

    @BeforeEach
    void setUp() {
        engine = new HashStorageEngine<>(
                new StorageConfig(16, 0.75f),
                new VersionGenerator()
        );
        indexManager = new IndexManager(engine);
        engine.registerIndexManager(indexManager);
        indexManager.start();
    }

    @AfterEach
    void tearDown() {
        if (indexManager != null) {
            indexManager.close();
        }
    }

    @Test
    void testSecondaryIndexExactMatch() {
        indexManager.registerIndex("city");

        engine.put("k1", "name=John,city=Boston");
        engine.put("k2", "name=Alice,city=Boston");
        engine.put("k3", "name=Bob,city=NewYork");

        Set<String> bostonKeys = indexManager.searchEquals("city", "Boston");
        assertEquals(2, bostonKeys.size());
        assertTrue(bostonKeys.contains("k1"));
        assertTrue(bostonKeys.contains("k2"));

        Set<String> nyKeys = indexManager.searchEquals("city", "NewYork");
        assertEquals(1, nyKeys.size());
        assertTrue(nyKeys.contains("k3"));

        // Update record: change k1 city to Chicago
        engine.put("k1", "name=John,city=Chicago");

        bostonKeys = indexManager.searchEquals("city", "Boston");
        assertEquals(1, bostonKeys.size());
        assertFalse(bostonKeys.contains("k1")); // removed from Boston
        assertTrue(bostonKeys.contains("k2"));

        Set<String> chicagoKeys = indexManager.searchEquals("city", "Chicago");
        assertEquals(1, chicagoKeys.size());
        assertTrue(chicagoKeys.contains("k1"));

        // Delete record
        engine.delete("k2");
        bostonKeys = indexManager.searchEquals("city", "Boston");
        assertTrue(bostonKeys.isEmpty());
    }

    @Test
    void testRangeIndex() {
        indexManager.registerIndex("age");

        engine.put("k1", "name=John,age=20");
        engine.put("k2", "name=Alice,age=30");
        engine.put("k3", "name=Bob,age=40");

        // Range query: age 25 to 45
        Set<String> results = indexManager.searchRange("age", "25", "45");
        assertEquals(2, results.size());
        assertTrue(results.contains("k2"));
        assertTrue(results.contains("k3"));

        // Unbounded end: age 25+
        results = indexManager.searchRange("age", "25", null);
        assertEquals(2, results.size());
        assertTrue(results.contains("k2"));
        assertTrue(results.contains("k3"));

        // Unbounded start: age up to 35
        results = indexManager.searchRange("age", null, "35");
        assertEquals(2, results.size());
        assertTrue(results.contains("k1"));
        assertTrue(results.contains("k2"));
    }

    @Test
    void testPrefixTrieIndex() {
        indexManager.registerPrefixIndex("name");

        engine.put("k1", "name=Alexander,city=Boston");
        engine.put("k2", "name=Alex,city=Chicago");
        engine.put("k3", "name=Bob,city=NewYork");

        Set<String> results = indexManager.searchPrefix("name", "Ale");
        assertEquals(2, results.size());
        assertTrue(results.contains("k1"));
        assertTrue(results.contains("k2"));

        results = indexManager.searchPrefix("name", "Alex");
        assertEquals(2, results.size());
        assertTrue(results.contains("k1"));
        assertTrue(results.contains("k2"));

        results = indexManager.searchPrefix("name", "Alexander");
        assertEquals(1, results.size());
        assertTrue(results.contains("k1"));

        results = indexManager.searchPrefix("name", "Bo");
        assertEquals(1, results.size());
        assertTrue(results.contains("k3"));

        results = indexManager.searchPrefix("name", "Charlie");
        assertTrue(results.isEmpty());

        // Update k1 name to something else
        engine.put("k1", "name=Arthur,city=Boston");
        results = indexManager.searchPrefix("name", "Ale");
        assertEquals(1, results.size()); // only Alex left
        assertFalse(results.contains("k1"));

        results = indexManager.searchPrefix("name", "Art");
        assertEquals(1, results.size());
        assertTrue(results.contains("k1"));
    }

    @Test
    void testTtlIndexExpiration() throws InterruptedException {
        // Put k1 with 100ms TTL
        engine.put("k1", "name=TempKey,ttl=100");
        
        // Put k2 with no TTL
        engine.put("k2", "name=PersistKey");

        assertEquals("name=TempKey,ttl=100", engine.get("k1"));
        assertEquals("name=PersistKey", engine.get("k2"));

        // Wait 600ms for background cleaner to run (runs every 500ms)
        Thread.sleep(600);

        // k1 should be gone, k2 should remain
        assertNull(engine.get("k1"));
        assertEquals("name=PersistKey", engine.get("k2"));
    }
}
