package com.atlasdb.backup;

import com.atlasdb.security.crypto.AesEngine;
import com.atlasdb.storage.Entry;
import com.atlasdb.storage.HashStorageEngine;
import com.atlasdb.storage.VersionNode;
import com.atlasdb.storage.persistence.WalEntry;
import com.atlasdb.storage.persistence.WriteAheadLog;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Manages full snapshots, incremental log backups, GZIP compressions,
 * AES encryptions, and Point-in-Time Recovery (PITR) playbacks.
 */
public final class BackupManager {

    /**
     * Captures a full database state snapshot, compresses it with GZIP,
     * encrypts it using AES-GCM, and saves it to the target file.
     */
    public static void captureSnapshot(HashStorageEngine<String, String> engine, File targetFile, AesEngine aes) throws IOException {
        byte[] rawState = serializeEngineState(engine);
        byte[] compressed = compress(rawState);
        byte[] encrypted = aes.encrypt(compressed);

        try (FileOutputStream fos = new FileOutputStream(targetFile)) {
            fos.write(encrypted);
        }
    }

    /**
     * Restores database state from an encrypted and compressed snapshot file.
     */
    public static void restoreSnapshot(HashStorageEngine<String, String> engine, File sourceFile, AesEngine aes) throws IOException {
        byte[] encrypted;
        try (FileInputStream fis = new FileInputStream(sourceFile)) {
            encrypted = fis.readAllBytes();
        }

        byte[] compressed = aes.decrypt(encrypted);
        byte[] rawState = decompress(compressed);
        restoreEngineState(engine, rawState);
    }

    /**
     * Backs up incremental WAL log mutations occurring after sinceTimestamp.
     * Compresses, encrypts, and saves the backup payload to targetFile.
     */
    public static void captureIncrementalBackup(File walDir, long sinceTimestamp, File targetFile, AesEngine aes) throws IOException {
        byte[] rawState = serializeIncrementalBackup(walDir, sinceTimestamp);
        byte[] compressed = compress(rawState);
        byte[] encrypted = aes.encrypt(compressed);

        try (FileOutputStream fos = new FileOutputStream(targetFile)) {
            fos.write(encrypted);
        }
    }

    /**
     * Reads and decrypts/decompress a backup payload file.
     */
    public static byte[] loadBackupBytes(File sourceFile, AesEngine aes) throws IOException {
        byte[] encrypted;
        try (FileInputStream fis = new FileInputStream(sourceFile)) {
            encrypted = fis.readAllBytes();
        }
        byte[] compressed = aes.decrypt(encrypted);
        return decompress(compressed);
    }

    /**
     * Performs Point-in-Time Recovery (PITR) up to the target timestamp using base snapshot
     * and a chronological list of incremental backups.
     */
    public static void recoverToPointInTime(HashStorageEngine<String, String> engine,
                                           byte[] baseSnapshotBytes,
                                           List<byte[]> incrementalBackups,
                                           long targetTimestamp) throws IOException {
        engine.clear();

        List<BackupRecord> allRecords = new ArrayList<>();

        // Parse base snapshot records
        if (baseSnapshotBytes != null && baseSnapshotBytes.length > 0) {
            allRecords.addAll(deserializeRecords(baseSnapshotBytes, targetTimestamp));
        }

        // Parse incremental records
        for (byte[] incBytes : incrementalBackups) {
            allRecords.addAll(deserializeRecords(incBytes, targetTimestamp));
        }

        // Sort all mutations chronologically (oldest first) to build state correctly
        allRecords.sort(Comparator.comparingLong(BackupRecord::timestamp));

        // Replay mutations sequentially to state machine
        for (BackupRecord rec : allRecords) {
            if (rec.isDelete()) {
                engine.delete(rec.key(), rec.timestamp());
            } else {
                engine.put(rec.key(), rec.value(), rec.timestamp());
            }
        }
    }

    private static byte[] serializeEngineState(HashStorageEngine<String, String> engine) throws IOException {
        List<BackupRecord> records = new ArrayList<>();
        Iterator<Entry<String, String>> it = engine.iterator();
        while (it.hasNext()) {
            Entry<String, String> entry = it.next();
            String key = entry.getKey();
            VersionNode<String> node = entry.getVersionHead();
            while (node != null) {
                records.add(new BackupRecord(key, node.getValue(), node.getTimestamp(), node.isDeleted()));
                node = node.getNext();
            }
        }

        // Sort chronologically (oldest first)
        records.sort(Comparator.comparingLong(BackupRecord::timestamp));

        return serializeRecords(records);
    }

    private static void restoreEngineState(HashStorageEngine<String, String> engine, byte[] stateBytes) throws IOException {
        engine.clear();
        List<BackupRecord> records = deserializeRecords(stateBytes, Long.MAX_VALUE);
        // Sort chronologically (oldest first)
        records.sort(Comparator.comparingLong(BackupRecord::timestamp));

        for (BackupRecord rec : records) {
            if (rec.isDelete()) {
                engine.delete(rec.key(), rec.timestamp());
            } else {
                engine.put(rec.key(), rec.value(), rec.timestamp());
            }
        }
    }

    private static byte[] serializeIncrementalBackup(File walDir, long sinceTimestamp) throws IOException {
        List<BackupRecord> records = new ArrayList<>();
        try (WriteAheadLog wal = new WriteAheadLog(walDir, false)) {
            List<WalEntry> entries = wal.readAll();
            for (WalEntry entry : entries) {
                if (entry.getVersion() > sinceTimestamp) {
                    boolean isDelete = entry.getType() == WalEntry.TYPE_DELETE;
                    records.add(new BackupRecord(entry.getKey(), entry.getValue(), entry.getVersion(), isDelete));
                }
            }
        }

        // Sort chronologically (oldest first)
        records.sort(Comparator.comparingLong(BackupRecord::timestamp));

        return serializeRecords(records);
    }

    private static byte[] serializeRecords(List<BackupRecord> records) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (DataOutputStream dos = new DataOutputStream(baos)) {
            dos.writeInt(records.size());
            for (BackupRecord rec : records) {
                dos.writeUTF(rec.key());
                dos.writeLong(rec.timestamp());
                dos.writeBoolean(rec.isDelete());
                if (!rec.isDelete()) {
                    dos.writeUTF(rec.value());
                }
            }
        }
        return baos.toByteArray();
    }

    private static List<BackupRecord> deserializeRecords(byte[] data, long maxTimestamp) throws IOException {
        List<BackupRecord> records = new ArrayList<>();
        ByteArrayInputStream bais = new ByteArrayInputStream(data);
        try (DataInputStream dis = new DataInputStream(bais)) {
            int count = dis.readInt();
            for (int i = 0; i < count; i++) {
                String key = dis.readUTF();
                long timestamp = dis.readLong();
                boolean isDelete = dis.readBoolean();
                String value = null;
                if (!isDelete) {
                    value = dis.readUTF();
                }

                if (timestamp <= maxTimestamp) {
                    records.add(new BackupRecord(key, value, timestamp, isDelete));
                }
            }
        }
        return records;
    }

    private static byte[] compress(byte[] input) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (GZIPOutputStream gzos = new GZIPOutputStream(baos)) {
            gzos.write(input);
        }
        return baos.toByteArray();
    }

    private static byte[] decompress(byte[] input) throws IOException {
        ByteArrayInputStream bais = new ByteArrayInputStream(input);
        try (GZIPInputStream gzis = new GZIPInputStream(bais)) {
            return gzis.readAllBytes();
        }
    }

    private static record BackupRecord(
            String key,
            String value,
            long timestamp,
            boolean isDelete
    ) {}
}
