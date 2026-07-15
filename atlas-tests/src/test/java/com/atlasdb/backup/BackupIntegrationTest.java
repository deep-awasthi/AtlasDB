package com.atlasdb.backup;

import com.atlasdb.common.VersionGenerator;
import com.atlasdb.security.crypto.AesEngine;
import com.atlasdb.storage.HashStorageEngine;
import com.atlasdb.storage.config.StorageConfig;
import com.atlasdb.storage.persistence.WalEntry;
import com.atlasdb.storage.persistence.WriteAheadLog;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.crypto.SecretKey;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class BackupIntegrationTest {

    @Test
    void testSnapshotBackupAndRestore(@TempDir Path tempDir) throws Exception {
        File snapshotFile = tempDir.resolve("snapshot.bin.enc").toFile();

        // 1. Initialize engine and write data
        VersionGenerator vg = new VersionGenerator();
        HashStorageEngine<String, String> engine = new HashStorageEngine<>(new StorageConfig(4, 0.75f), vg);

        engine.put("k1", "v1", 100);
        engine.put("k2", "v2", 200);

        SecretKey key = AesEngine.generateKey();
        AesEngine aes = new AesEngine(key);

        // 2. Capture Snapshot
        BackupManager.captureSnapshot(engine, snapshotFile, aes);
        assertTrue(snapshotFile.exists());
        assertTrue(snapshotFile.length() > 0);

        // 3. Clear engine and restore
        engine.clear();
        assertNull(engine.get("k1"));
        assertNull(engine.get("k2"));

        BackupManager.restoreSnapshot(engine, snapshotFile, aes);

        // 4. Assert restored state
        assertEquals("v1", engine.get("k1"));
        assertEquals("v2", engine.get("k2"));
        assertEquals(100, engine.getVersion("k1"));
        assertEquals(200, engine.getVersion("k2"));
    }

    @Test
    void testIncrementalBackupAndPointInTimeRecovery(@TempDir Path tempDir) throws Exception {
        File snapshotFile = tempDir.resolve("snapshot.bin.enc").toFile();
        File incrementalFile = tempDir.resolve("incremental.bin.enc").toFile();
        File walDir = tempDir.resolve("wal").toFile();

        SecretKey key = AesEngine.generateKey();
        AesEngine aes = new AesEngine(key);

        // 1. Create base snapshot at timestamp 100
        VersionGenerator vg = new VersionGenerator();
        HashStorageEngine<String, String> engine = new HashStorageEngine<>(new StorageConfig(4, 0.75f), vg);
        engine.put("k1", "v1", 50);
        engine.put("k2", "v2", 100);

        BackupManager.captureSnapshot(engine, snapshotFile, aes);

        // 2. Perform write mutations via Write-Ahead Log
        try (WriteAheadLog wal = new WriteAheadLog(walDir, true)) {
            wal.append(new WalEntry(WalEntry.TYPE_PUT, 150, "k1", "v1_new"));
            wal.append(new WalEntry(WalEntry.TYPE_PUT, 250, "k3", "v3"));
            wal.append(new WalEntry(WalEntry.TYPE_DELETE, 350, "k2", null));
        }

        // 3. Capture Incremental Backup since timestamp 100
        BackupManager.captureIncrementalBackup(walDir, 100, incrementalFile, aes);
        assertTrue(incrementalFile.exists());

        // Load backup payloads
        byte[] snapshotBytes = BackupManager.loadBackupBytes(snapshotFile, aes);
        byte[] incrementalBytes = BackupManager.loadBackupBytes(incrementalFile, aes);

        // 4. Test Point-in-Time Recovery (PITR) up to version 300
        // PITR 300 should contain k1="v1_new" (150), k3="v3" (250), and k2="v2" (100)
        // because delete of k2 happened at 350 (which is > 300)
        BackupManager.recoverToPointInTime(engine, snapshotBytes, List.of(incrementalBytes), 300);

        assertEquals("v1_new", engine.get("k1"));
        assertEquals("v2", engine.get("k2"));
        assertEquals("v3", engine.get("k3"));

        // 5. Test PITR up to version 400 (includes deletion of k2 at 350)
        BackupManager.recoverToPointInTime(engine, snapshotBytes, List.of(incrementalBytes), 400);

        assertEquals("v1_new", engine.get("k1"));
        assertNull(engine.get("k2"), "k2 should be tombstone-deleted at version 400");
        assertEquals("v3", engine.get("k3"));
    }
}
