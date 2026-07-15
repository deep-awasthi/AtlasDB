package com.atlasdb.network.protocol;

import java.util.Arrays;
import java.util.Objects;

/**
 * Represents a binary message frame in the AtlasDB custom network protocol.
 * Holds protocol metadata, session metrics, payload integrity checksum, and payload bytes.
 */
public final class Packet {

    public static final byte MAGIC = (byte) 0xAD;
    public static final byte VERSION = (byte) 0x01;

    // Packet Types
    public static final byte TYPE_HEARTBEAT = 0x01;
    public static final byte TYPE_REQ_PUT = 0x02;
    public static final byte TYPE_REQ_GET = 0x03;
    public static final byte TYPE_REQ_DELETE = 0x04;
    public static final byte TYPE_RESP_SUCCESS = 0x05;
    public static final byte TYPE_RESP_ERROR = 0x06;
    public static final byte TYPE_REQ_SQL = 0x07;

    private final byte type;
    private final long requestId;
    private final long checksum;
    private final byte[] payload;

    /**
     * Constructs a Packet.
     *
     * @param type      the packet type indicator
     * @param requestId the unique monotonic request identifier
     * @param checksum  the integrity checksum of the payload
     * @param payload   the binary payload content
     */
    public Packet(byte type, long requestId, long checksum, byte[] payload) {
        this.type = type;
        this.requestId = requestId;
        this.checksum = checksum;
        this.payload = payload == null ? new byte[0] : payload;
    }

    /**
     * Retrieves the protocol magic byte.
     *
     * @return the magic byte constant
     */
    public byte getMagic() {
        return MAGIC;
    }

    /**
     * Retrieves the protocol version byte.
     *
     * @return the version byte constant
     */
    public byte getVersion() {
        return VERSION;
    }

    /**
     * Retrieves the packet type.
     *
     * @return the type byte
     */
    public byte getType() {
        return type;
    }

    /**
     * Retrieves the request ID.
     *
     * @return the unique 64-bit request identifier
     */
    public long getRequestId() {
        return requestId;
    }

    /**
     * Retrieves the integrity checksum.
     *
     * @return the checksum value
     */
    public long getChecksum() {
        return checksum;
    }

    /**
     * Retrieves the payload length.
     *
     * @return the size of the payload in bytes
     */
    public int getPayloadLength() {
        return payload.length;
    }

    /**
     * Retrieves a copy of the payload bytes.
     *
     * @return the payload array
     */
    public byte[] getPayload() {
        return payload;
    }

    @Override
    public String toString() {
        return "Packet{" +
                "type=" + type +
                ", requestId=" + requestId +
                ", checksum=" + checksum +
                ", payloadLength=" + payload.length +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Packet packet = (Packet) o;
        return type == packet.type &&
                requestId == packet.requestId &&
                checksum == packet.checksum &&
                Arrays.equals(payload, packet.payload);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(type, requestId, checksum);
        result = 31 * result + Arrays.hashCode(payload);
        return result;
    }
}
