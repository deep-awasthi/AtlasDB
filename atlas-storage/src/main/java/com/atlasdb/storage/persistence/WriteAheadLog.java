package com.atlasdb.storage.persistence;

import com.atlasdb.common.exception.StorageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32;

/**
 * Write-Ahead Log (WAL) manager for AtlasDB.
 * Handles durability of write operations with checksum verification and synchronous/buffered writes.
 */
public final class WriteAheadLog implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(WriteAheadLog.class);

    private static final byte WAL_MAGIC = (byte) 0x57; // 'W'
    private final File walFile;
    private final boolean syncOnWrite;

    private RandomAccessFile raf;
    private FileChannel channel;

    /**
     * Constructs a WriteAheadLog.
     *
     * @param dataDir     the data directory where the log file is stored
     * @param syncOnWrite if true, executes fsync to disk on every append
     * @throws IOException if the file cannot be created or opened
     */
    public WriteAheadLog(File dataDir, boolean syncOnWrite) throws IOException {
        if (!dataDir.exists() && !dataDir.mkdirs()) {
            throw new IOException("Failed to create WAL directory: " + dataDir.getAbsolutePath());
        }
        this.walFile = new File(dataDir, "wal.log");
        this.syncOnWrite = syncOnWrite;
        open();
    }

    private void open() throws IOException {
        this.raf = new RandomAccessFile(walFile, "rw");
        this.channel = raf.getChannel();
        // Seek to end to allow append-only writes
        channel.position(channel.size());
    }

    /**
     * Appends a record to the write-ahead log.
     *
     * @param entry the WAL entry to write
     * @throws IOException if writing to disk fails
     */
    public synchronized void append(WalEntry entry) throws IOException {
        byte[] keyBytes = entry.getKey().getBytes(StandardCharsets.UTF_8);
        byte[] valBytes = entry.getType() == WalEntry.TYPE_PUT
                ? entry.getValue().getBytes(StandardCharsets.UTF_8)
                : new byte[0];

        // Format size: Magic(1) + Checksum(8) + Type(1) + Version(8) + KeyLen(4) + Key + ValLen(4) + Val
        int payloadSize = 1 + 8 + 4 + keyBytes.length + 4 + valBytes.length; // excluding Magic and Checksum fields for checksum calculation
        ByteBuffer payloadBuffer = ByteBuffer.allocate(payloadSize);
        payloadBuffer.put(entry.getType());
        payloadBuffer.putLong(entry.getVersion());
        payloadBuffer.putInt(keyBytes.length);
        payloadBuffer.put(keyBytes);
        payloadBuffer.putInt(valBytes.length);
        if (valBytes.length > 0) {
            payloadBuffer.put(valBytes);
        }
        payloadBuffer.flip();

        // Calculate CRC32 checksum over the payload
        CRC32 crc = new CRC32();
        crc.update(payloadBuffer.array(), 0, payloadBuffer.limit());
        long checksum = crc.getValue();

        // Write magic + checksum + payload to disk
        ByteBuffer writeBuffer = ByteBuffer.allocate(1 + 8 + payloadSize);
        writeBuffer.put(WAL_MAGIC);
        writeBuffer.putLong(checksum);
        writeBuffer.put(payloadBuffer);
        writeBuffer.flip();

        while (writeBuffer.hasRemaining()) {
            channel.write(writeBuffer);
        }

        if (syncOnWrite) {
            channel.force(false);
        }
    }

    /**
     * Flushes current writes in the channel to disk (fsync).
     *
     * @throws IOException if fsync fails
     */
    public synchronized void flush() throws IOException {
        if (channel != null && channel.isOpen()) {
            channel.force(false);
        }
    }

    /**
     * Reads all valid entries from the WAL file.
     * If corruption (invalid checksum or partial write) is encountered,
     * reads up to the corruption point and truncates the file there.
     *
     * @return list of recovered WalEntry objects
     * @throws IOException if disk read operations fail
     */
    public synchronized List<WalEntry> readAll() throws IOException {
        List<WalEntry> entries = new ArrayList<>();
        if (channel.size() == 0) {
            return entries;
        }

        channel.position(0);
        long truncatePosition = 0;
        ByteBuffer headerBuffer = ByteBuffer.allocate(9); // Magic (1) + Checksum (8)

        try {
            while (true) {
                headerBuffer.clear();
                int read = channel.read(headerBuffer);
                if (read == -1) {
                    break; // EOF
                }
                if (read < 9) {
                    logger.warn("Partial WAL record header encountered at offset {}. Truncating log.", channel.position() - read);
                    break; // Partial header
                }

                headerBuffer.flip();
                byte magic = headerBuffer.get();
                if (magic != WAL_MAGIC) {
                    logger.warn("Invalid WAL magic byte (0x{}) encountered at offset {}. Truncating log.", 
                            Integer.toHexString(magic & 0xFF), channel.position() - 9);
                    break;
                }

                long expectedChecksum = headerBuffer.getLong();

                // Read Type(1) + Version(8) + KeyLen(4)
                ByteBuffer metaBuffer = ByteBuffer.allocate(13);
                read = channel.read(metaBuffer);
                if (read < 13) {
                    logger.warn("Partial WAL record metadata encountered. Truncating log.");
                    break;
                }
                metaBuffer.flip();
                byte type = metaBuffer.get();
                long version = metaBuffer.getLong();
                int keyLen = metaBuffer.getInt();

                if (keyLen <= 0 || keyLen > 1024 * 1024) {
                    logger.warn("Corrupted WAL entry key length ({}) at offset {}. Truncating.", keyLen, channel.position() - 13);
                    break;
                }

                // Read key + ValLen(4)
                ByteBuffer keyAndValLenBuffer = ByteBuffer.allocate(keyLen + 4);
                read = channel.read(keyAndValLenBuffer);
                if (read < keyLen + 4) {
                    logger.warn("Partial WAL record key/value-length metadata. Truncating log.");
                    break;
                }
                keyAndValLenBuffer.flip();
                byte[] keyBytes = new byte[keyLen];
                keyAndValLenBuffer.get(keyBytes);
                int valLen = keyAndValLenBuffer.getInt();

                if (valLen < 0 || valLen > 10 * 1024 * 1024) {
                    logger.warn("Corrupted WAL entry value length ({}) at offset {}. Truncating.", valLen, channel.position() - 4);
                    break;
                }

                // Read value bytes
                ByteBuffer valBuffer = ByteBuffer.allocate(valLen);
                if (valLen > 0) {
                    read = channel.read(valBuffer);
                    if (read < valLen) {
                        logger.warn("Partial WAL record value payload. Truncating log.");
                        break;
                    }
                    valBuffer.flip();
                }

                byte[] valBytes = new byte[valLen];
                if (valLen > 0) {
                    valBuffer.get(valBytes);
                }

                // Calculate checksum to verify integrity
                int payloadSize = 1 + 8 + 4 + keyLen + 4 + valLen;
                ByteBuffer checkBuffer = ByteBuffer.allocate(payloadSize);
                checkBuffer.put(type);
                checkBuffer.putLong(version);
                checkBuffer.putInt(keyLen);
                checkBuffer.put(keyBytes);
                checkBuffer.putInt(valLen);
                if (valLen > 0) {
                    checkBuffer.put(valBytes);
                }
                checkBuffer.flip();

                CRC32 crc = new CRC32();
                crc.update(checkBuffer.array(), 0, checkBuffer.limit());
                long actualChecksum = crc.getValue();

                if (actualChecksum != expectedChecksum) {
                    logger.warn("WAL checksum mismatch (expected: {}, actual: {}). Log is corrupted, truncating.", 
                            expectedChecksum, actualChecksum);
                    break;
                }

                String key = new String(keyBytes, StandardCharsets.UTF_8);
                String val = valLen > 0 ? new String(valBytes, StandardCharsets.UTF_8) : null;
                entries.add(new WalEntry(type, version, key, val));

                truncatePosition = channel.position();
            }
        } catch (Exception e) {
            logger.error("Error reading WAL entries: {}", e.getMessage(), e);
        }

        // Truncate any corrupted trailing logs
        if (truncatePosition < channel.size()) {
            logger.warn("Truncating WAL to clean offset {}", truncatePosition);
            channel.truncate(truncatePosition);
            channel.position(truncatePosition);
        }

        return entries;
    }

    /**
     * Clears and truncates the WAL to start logging in a fresh file.
     *
     * @throws IOException if truncation fails
     */
    public synchronized void truncate() throws IOException {
        close();
        if (walFile.exists() && !walFile.delete()) {
            throw new IOException("Failed to delete WAL file for truncation: " + walFile.getAbsolutePath());
        }
        open();
    }

    @Override
    public synchronized void close() throws IOException {
        if (channel != null) {
            try {
                channel.close();
            } finally {
                channel = null;
            }
        }
        if (raf != null) {
            try {
                raf.close();
            } finally {
                raf = null;
            }
        }
    }
}
