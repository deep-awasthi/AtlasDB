package com.atlasdb.storage.persistence;

import com.atlasdb.common.exception.StorageException;
import com.atlasdb.storage.Entry;
import com.atlasdb.storage.HashStorageEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Handles creation and recovery of compressed database snapshots.
 * Uses GZIP compression to minimize disk space consumption.
 */
public final class SnapshotManager {

    private static final Logger logger = LoggerFactory.getLogger(SnapshotManager.class);

    private static final byte[] SNAPSHOT_MAGIC = {(byte) 0x41, (byte) 0x54, (byte) 0x4C, (byte) 0x53}; // "ATLS"
    private final File snapshotFile;
    private final File tempSnapshotFile;

    /**
     * Constructs a SnapshotManager.
     *
     * @param dataDir the directory containing database persistence files
     */
    public SnapshotManager(File dataDir) {
        if (!dataDir.exists() && !dataDir.mkdirs()) {
            logger.warn("Failed to create snapshot directory: {}", dataDir.getAbsolutePath());
        }
        this.snapshotFile = new File(dataDir, "snapshot.bin");
        this.tempSnapshotFile = new File(dataDir, "snapshot.tmp");
    }

    /**
     * Captures a compressed snapshot of the database state.
     * Writes to a temporary file first and renames atomically to prevent corrupt snapshot creation.
     *
     * @param snapshotVersion the monotonic version threshold representing the snapshot time
     * @param entries         an iterable collection of all active key-value entries
     * @throws IOException if disk serialization fails
     */
    public synchronized void createSnapshot(long snapshotVersion, Iterable<Entry<String, String>> entries) throws IOException {
        // Count entries first to write to header
        int entryCount = 0;
        for (Entry<String, String> ignored : entries) {
            entryCount++;
        }

        try (OutputStream fileOut = new FileOutputStream(tempSnapshotFile);
             BufferedOutputStream buffOut = new BufferedOutputStream(fileOut);
             GZIPOutputStream gzipOut = new GZIPOutputStream(buffOut);
             DataOutputStream dataOut = new DataOutputStream(gzipOut)) {

            // Header: Magic(4) + Version(8) + Count(4)
            dataOut.write(SNAPSHOT_MAGIC);
            dataOut.writeLong(snapshotVersion);
            dataOut.writeInt(entryCount);

            // Payload: Key length + Key + Value length + Value + Version
            for (Entry<String, String> entry : entries) {
                byte[] keyBytes = entry.getKey().getBytes(StandardCharsets.UTF_8);
                byte[] valBytes = entry.getValue().getBytes(StandardCharsets.UTF_8);

                dataOut.writeInt(keyBytes.length);
                dataOut.write(keyBytes);
                dataOut.writeInt(valBytes.length);
                dataOut.write(valBytes);
                dataOut.writeLong(entry.getVersion());
            }

            dataOut.flush();
        }

        // Rename atomically to overwrite the active snapshot
        Files.move(tempSnapshotFile.toPath(), snapshotFile.toPath(), 
                StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);

        logger.info("Successfully created compressed snapshot at version {} (records: {})", snapshotVersion, entryCount);
    }

    /**
     * Loads the latest snapshot into the provided HashStorageEngine.
     *
     * @param engine the target storage engine to populate
     * @return the snapshot version restored, or -1 if no snapshot exists
     * @throws IOException if reading or decompressing the snapshot fails
     */
    public synchronized long loadSnapshot(HashStorageEngine<String, String> engine) throws IOException {
        if (!snapshotFile.exists()) {
            logger.info("No snapshot file found at {}. Starting fresh.", snapshotFile.getAbsolutePath());
            return -1L;
        }

        logger.info("Loading database snapshot from {}...", snapshotFile.getAbsolutePath());
        long snapshotVersion;
        int entryCount;

        try (InputStream fileIn = new FileInputStream(snapshotFile);
             BufferedInputStream buffIn = new BufferedInputStream(fileIn);
             GZIPInputStream gzipIn = new GZIPInputStream(buffIn);
             DataInputStream dataIn = new DataInputStream(gzipIn)) {

            // Read and verify header magic
            byte[] magic = new byte[4];
            dataIn.readFully(magic);
            for (int i = 0; i < 4; i++) {
                if (magic[i] != SNAPSHOT_MAGIC[i]) {
                    throw new StorageException("Snapshot magic header mismatch. Corrupt file.");
                }
            }

            snapshotVersion = dataIn.readLong();
            entryCount = dataIn.readInt();

            logger.info("Snapshot header verified. Version: {}, Expected records: {}", snapshotVersion, entryCount);

            // Clear the existing engine before loading snapshot
            engine.clear();

            for (int i = 0; i < entryCount; i++) {
                int keyLen = dataIn.readInt();
                byte[] keyBytes = new byte[keyLen];
                dataIn.readFully(keyBytes);

                int valLen = dataIn.readInt();
                byte[] valBytes = new byte[valLen];
                dataIn.readFully(valBytes);

                long entryVersion = dataIn.readLong();

                String key = new String(keyBytes, StandardCharsets.UTF_8);
                String val = new String(valBytes, StandardCharsets.UTF_8);

                // Populate directly into the database engine bypass
                engine.loadEntry(key, val, entryVersion);
            }
        }

        logger.info("Restored {} records from snapshot successfully.", entryCount);
        return snapshotVersion;
    }

    /**
     * Checks if a snapshot exists.
     *
     * @return true if file exists, false otherwise
     */
    public boolean snapshotExists() {
        return snapshotFile.exists();
    }
}
