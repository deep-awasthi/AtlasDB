package com.atlasdb.cluster.protocol;

import com.atlasdb.cluster.ClusterNode;
import com.atlasdb.cluster.NodeState;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Handles custom binary serialization and deserialization of clustering protocol payloads.
 */
public final class ClusterMessage {

    // Message Types
    public static final byte TYPE_JOIN = 0x10;
    public static final byte TYPE_HEARTBEAT = 0x11;
    public static final byte TYPE_HEARTBEAT_ACK = 0x12;
    public static final byte TYPE_METADATA_SYNC = 0x13;

    private ClusterMessage() {
        // Utility constructor
    }

    /**
     * Serializes a Node JOIN request.
     */
    public static byte[] encodeJoin(String nodeId, String host, int port) {
        byte[] idBytes = nodeId.getBytes(StandardCharsets.UTF_8);
        byte[] hostBytes = host.getBytes(StandardCharsets.UTF_8);

        ByteBuffer buf = ByteBuffer.allocate(4 + idBytes.length + 4 + hostBytes.length + 4);
        buf.putInt(idBytes.length);
        buf.put(idBytes);
        buf.putInt(hostBytes.length);
        buf.put(hostBytes);
        buf.putInt(port);
        return buf.array();
    }

    /**
     * Deserializes a Node JOIN request.
     */
    public static ClusterNode decodeJoin(byte[] payload) {
        ByteBuffer buf = ByteBuffer.wrap(payload);
        int idLen = buf.getInt();
        byte[] idBytes = new byte[idLen];
        buf.get(idBytes);
        int hostLen = buf.getInt();
        byte[] hostBytes = new byte[hostLen];
        buf.get(hostBytes);
        int port = buf.getInt();

        return new ClusterNode(
                new String(idBytes, StandardCharsets.UTF_8),
                new String(hostBytes, StandardCharsets.UTF_8),
                port,
                NodeState.STARTING
        );
    }

    /**
     * Serializes a Heartbeat ping or ack message carrying senderId.
     */
    public static byte[] encodeHeartbeat(String senderId) {
        byte[] idBytes = senderId.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buf = ByteBuffer.allocate(4 + idBytes.length);
        buf.putInt(idBytes.length);
        buf.put(idBytes);
        return buf.array();
    }

    /**
     * Deserializes a Heartbeat message.
     */
    public static String decodeHeartbeat(byte[] payload) {
        ByteBuffer buf = ByteBuffer.wrap(payload);
        int idLen = buf.getInt();
        byte[] idBytes = new byte[idLen];
        buf.get(idBytes);
        return new String(idBytes, StandardCharsets.UTF_8);
    }

    /**
     * Serializes cluster node membership list.
     */
    public static byte[] encodeMetadataSync(List<ClusterNode> nodes) {
        // Calculate dynamic size first
        int size = 4; // nodes count
        List<byte[]> idBytesList = new ArrayList<>();
        List<byte[]> hostBytesList = new ArrayList<>();

        for (ClusterNode node : nodes) {
            byte[] idBytes = node.getNodeId().getBytes(StandardCharsets.UTF_8);
            byte[] hostBytes = node.getHost().getBytes(StandardCharsets.UTF_8);
            idBytesList.add(idBytes);
            hostBytesList.add(hostBytes);

            size += 4 + idBytes.length + 4 + hostBytes.length + 4 + 4 + 8;
        }

        ByteBuffer buf = ByteBuffer.allocate(size);
        buf.putInt(nodes.size());
        for (int i = 0; i < nodes.size(); i++) {
            ClusterNode node = nodes.get(i);
            byte[] idBytes = idBytesList.get(i);
            byte[] hostBytes = hostBytesList.get(i);

            buf.putInt(idBytes.length);
            buf.put(idBytes);
            buf.putInt(hostBytes.length);
            buf.put(hostBytes);
            buf.putInt(node.getPort());
            buf.putInt(node.getState().ordinal());
            buf.putLong(node.getLastSeenTimestamp());
        }

        return buf.array();
    }

    /**
     * Deserializes cluster node membership list.
     */
    public static List<ClusterNode> decodeMetadataSync(byte[] payload) {
        ByteBuffer buf = ByteBuffer.wrap(payload);
        int nodeCount = buf.getInt();
        List<ClusterNode> list = new ArrayList<>(nodeCount);

        for (int i = 0; i < nodeCount; i++) {
            int idLen = buf.getInt();
            byte[] idBytes = new byte[idLen];
            buf.get(idBytes);
            int hostLen = buf.getInt();
            byte[] hostBytes = new byte[hostLen];
            buf.get(hostBytes);
            int port = buf.getInt();
            int stateOrd = buf.getInt();
            long lastSeen = buf.getLong();

            ClusterNode node = new ClusterNode(
                    new String(idBytes, StandardCharsets.UTF_8),
                    new String(hostBytes, StandardCharsets.UTF_8),
                    port,
                    NodeState.values()[stateOrd]
            );
            // Re-apply original lastSeen timestamp to keep nodes synchronized
            node.setLastSeenTimestamp(lastSeen);
            list.add(node);
        }

        return list;
    }
}
