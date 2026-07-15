package com.atlasdb.storage;

import com.atlasdb.common.VersionGenerator;
import com.atlasdb.storage.config.StorageConfig;
import com.atlasdb.storage.persistence.SnapshotManager;
import com.atlasdb.storage.persistence.WalEntry;
import com.atlasdb.storage.persistence.WriteAheadLog;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class PersistenceIntegrationTest {

    @TempDir
    Path tempDir;

    @Test
    void testWalLoggingAndReading() throws IOException {
        File dataDir = tempDir.toFile();
        try (WriteAheadLog wal = new WriteAheadLog(dataDir, true)) {
            wal.append(new WalEntry(WalEntry.TYPE_PUT, 100L, "k1", "v1"));
            wal.append(new WalEntry(WalEntry.TYPE_PUT, 101L, "k2", "v2"));
            wal.append(new WalEntry(WalEntry.TYPE_DELETE, 102L, "k1", null));
        }

        // Open again to read
        try (WriteAheadLog wal = new WriteAheadLog(dataDir, false)) {
            List<WalEntry> entries = wal.readAll();
            assertEquals(3, entries.size());

            assertEquals(WalEntry.TYPE_PUT, entries.get(0).getType());
            assertEquals(100L, entries.get(0).getVersion());
            assertEquals("k1", entries.get(0).getKey());
            assertEquals("v1", entries.get(0).getValue());

            assertEquals(WalEntry.TYPE_PUT, entries.get(1).getType());
            assertEquals(101L, entries.get(1).getVersion());
            assertEquals("k2", entries.get(1).getKey());
            assertEquals("v2", entries.get(1).getValue());

            assertEquals(WalEntry.TYPE_DELETE, entries.get(2).getType());
            assertEquals(102L, entries.get(2).getVersion());
            assertEquals("k1", entries.get(2).getKey());
            assertNull(entries.get(2).getValue());
        }
    }

    @Test
    void testSnapshotCreationAndLoading() throws IOException {
        File dataDir = tempDir.toFile();
        VersionGenerator versionGenerator = new VersionGenerator();
        HashStorageEngine<String, String> engine = new HashStorageEngine<>(
                new StorageConfig(8, 0.75f),
                versionGenerator
        );

        engine.put("k1", "v1");
        engine.put("k2", "v2");
        long checkpointVersion = versionGenerator.currentVersion();

        SnapshotManager snapshotManager = new SnapshotManager(dataDir);
        snapshotManager.createSnapshot(checkpointVersion, engine);

        // Clear the engine memory
        engine.clear();
        assertEquals(0, engine.size());

        // Load snapshot
        long restoredVersion = snapshotManager.loadSnapshot(engine);
        assertEquals(checkpointVersion, restoredVersion);
        assertEquals(2, engine.size());
        assertEquals("v1", engine.get("k1"));
        assertEquals("v2", engine.get("k2"));
        assertEquals(checkpointVersion, engine.getVersion("k2"));
    }

    @Test
    void testCrashRecoveryWorkflow() throws Exception {
        File dataDir = tempDir.toFile();
        VersionGenerator versionGenerator = new VersionGenerator();
        
        // 1. Initialize engine and WAL
        HashStorageEngine<String, String> engine = new HashStorageEngine<>(
                new StorageConfig(8, 0.75f),
                versionGenerator
        );
        WriteAheadLog wal = new WriteAheadLog(dataDir, true);
        engine.registerWal(wal);

        // Write initial data (this will be snapshotted)
        engine.put("k1", "v1");
        engine.put("k2", "v2");
        
        // 2. Perform Checkpoint (Snapshot + WAL truncate)
        long checkpointVersion = versionGenerator.currentVersion();
        SnapshotManager snapshotManager = new SnapshotManager(dataDir);
        snapshotManager.createSnapshot(checkpointVersion, engine);
        
        // Truncate WAL after snapshot
        wal.truncate();

        // 3. Write outstanding edits to WAL (post-checkpoint)
        engine.put("k3", "v3");
        engine.put("k1", "v1-updated");
        engine.delete("k2");

        // Close files to simulate safe database shutdown/crash
        wal.close();

        // 4. Recovery phase: instantiate fresh engine and replay
        HashStorageEngine<String, String> recoveredEngine = new HashStorageEngine<>(
                new StorageConfig(8, 0.75f),
                new VersionGenerator() // fresh version generator
        );

        // Load snapshot
        long restoredVersion = snapshotManager.loadSnapshot(recoveredEngine);
        assertEquals(checkpointVersion, restoredVersion);
        assertEquals(2, recoveredEngine.size());
        assertEquals("v1", recoveredEngine.get("k1"));
        assertEquals("v2", recoveredEngine.get("k2"));

        // Replay WAL
        try (WriteAheadLog walReplay = new WriteAheadLog(dataDir, false)) {
            List<WalEntry> entries = walReplay.readAll();
            for (WalEntry entry : entries) {
                if (entry.getVersion() > restoredVersion) {
                    if (entry.getType() == WalEntry.TYPE_PUT) {
                        recoveredEngine.loadEntry(entry.getKey(), entry.getValue(), entry.getVersion());
                    } else if (entry.getType() == WalEntry.TYPE_DELETE) {
                        recoveredEngine.delete(entry.getKey());
                    }
                }
            }
        }

        // 5. Assert final state after crash recovery
        assertEquals(2, recoveredEngine.size());
        assertEquals("v1-updated", recoveredEngine.get("k1"));
        assertNull(recoveredEngine.get("k2")); // Deleted
        assertEquals("v3", recoveredEngine.get("k3")); // New key from WAL
    }

    @Test
    void testWalCorruptionAutoTruncation() throws IOException {
        File dataDir = tempDir.toFile();
        
        // Write good entries
        try (WriteAheadLog wal = new WriteAheadLog(dataDir, true)) {
            wal.append(new WalEntry(WalEntry.TYPE_PUT, 100L, "k1", "v1"));
            wal.append(new WalEntry(WalEntry.TYPE_PUT, 101L, "k2", "v2"));
        }

        // Manually append garbage/corruption to simulate partial crash write
        File walFile = new File(dataDir, "wal.log");
        try (java.io.FileOutputStream fos = new java.io.FileOutputStream(walFile, true)) {
            fos.write(new byte[]{0x00, 0x11, 0x22, 0x33}); // corrupt tail
        }

        // Open WAL: it should auto-truncate and successfully read the 2 good entries
        try (WriteAheadLog wal = new WriteAheadLog(dataDir, false)) {
            List<WalEntry> entries = wal.readAll();
            assertEquals(2, entries.size());
            assertEquals("k1", entries.get(0).getKey());
            assertEquals("k2", entries.get(1).getKey());
        }
    }
}
