package com.atlasdb.network.protocol;

import java.nio.ByteBuffer;
import java.util.zip.CRC32;

/**
 * Utility class for encoding and decoding Packet instances to and from ByteBuffers.
 * Includes CRC32 validation to assert payload data integrity.
 */
public final class PacketCodec {

    /**
     * Header size in bytes:
     * - Magic: 1 byte
     * - Version: 1 byte
     * - Type: 1 byte
     * - Request ID: 8 bytes
     * - Checksum: 8 bytes
     * - Payload Length: 4 bytes
     * Total = 23 bytes.
     */
    public static final int HEADER_SIZE = 23;

    private PacketCodec() {
        // Prevent instantiation
    }

    /**
     * Encodes a Packet into a newly allocated ByteBuffer.
     * Computes the CRC32 checksum of the payload dynamically.
     *
     * @param packet the packet to encode
     * @return a ByteBuffer positioned at the beginning, containing the full encoded packet
     */
    public static ByteBuffer encode(Packet packet) {
        byte[] payload = packet.getPayload();
        long checksum = calculateChecksum(payload);

        int totalSize = HEADER_SIZE + payload.length;
        ByteBuffer buffer = ByteBuffer.allocate(totalSize);

        buffer.put(Packet.MAGIC);
        buffer.put(Packet.VERSION);
        buffer.put(packet.getType());
        buffer.putLong(packet.getRequestId());
        buffer.putLong(checksum);
        buffer.putInt(payload.length);
        buffer.put(payload);

        buffer.flip();
        return buffer;
    }

    /**
     * Attempts to decode a Packet from the provided ByteBuffer.
     * If the buffer does not contain a complete packet (header + payload),
     * it leaves the buffer unchanged and returns null.
     *
     * @param buffer the buffer containing incoming bytes
     * @return the decoded Packet, or null if there are insufficient bytes
     * @throws IllegalArgumentException if the magic number or protocol version is invalid
     * @throws IllegalStateException    if checksum verification fails
     */
    public static Packet decode(ByteBuffer buffer) {
        if (buffer.remaining() < HEADER_SIZE) {
            return null;
        }

        // Mark buffer position to reset if the payload is incomplete
        buffer.mark();

        byte magic = buffer.get();
        if (magic != Packet.MAGIC) {
            buffer.reset();
            throw new IllegalArgumentException(String.format("Invalid protocol magic byte: 0x%02X", magic));
        }

        byte version = buffer.get();
        if (version != Packet.VERSION) {
            buffer.reset();
            throw new IllegalArgumentException(String.format("Unsupported protocol version: 0x%02X", version));
        }

        byte type = buffer.get();
        long requestId = buffer.getLong();
        long checksum = buffer.getLong();
        int payloadLength = buffer.getInt();

        if (payloadLength < 0) {
            buffer.reset();
            throw new IllegalArgumentException("Negative payload length: " + payloadLength);
        }

        if (buffer.remaining() < payloadLength) {
            // Revert buffer position, waiting for more data to arrive
            buffer.reset();
            return null;
        }

        byte[] payload = new byte[payloadLength];
        buffer.get(payload);

        // Verify checksum integrity
        long calculated = calculateChecksum(payload);
        if (calculated != checksum) {
            throw new IllegalStateException(String.format(
                    "Integrity check failed: checksum mismatch (expected: %d, calculated: %d)",
                    checksum, calculated));
        }

        return new Packet(type, requestId, checksum, payload);
    }

    /**
     * Calculates CRC32 checksum for a payload array.
     *
     * @param payload the data bytes to hash
     * @return the CRC32 checksum value
     */
    public static long calculateChecksum(byte[] payload) {
        if (payload == null || payload.length == 0) {
            return 0L;
        }
        CRC32 crc = new CRC32();
        crc.update(payload);
        return crc.getValue();
    }
}
