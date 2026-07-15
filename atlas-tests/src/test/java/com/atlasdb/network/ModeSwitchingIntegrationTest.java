package com.atlasdb.network;

import com.atlasdb.common.VersionGenerator;
import com.atlasdb.network.client.TcpClient;
import com.atlasdb.network.protocol.Packet;
import com.atlasdb.network.server.TcpServer;
import com.atlasdb.storage.HashStorageEngine;
import com.atlasdb.storage.config.StorageConfig;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

class ModeSwitchingIntegrationTest {

    private int findFreePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    @Test
    void testNoSqlModeRestrictions() throws Exception {
        int port = findFreePort();
        HashStorageEngine<String, String> engine = new HashStorageEngine<>(
                new StorageConfig(16, 0.75f, "NOSQL"),
                new VersionGenerator()
        );

        TcpServer server = new TcpServer("127.0.0.1", port, engine);
        server.registerSqlQueryHandler(sql -> "dummy_sql_response");
        server.start();

        try (TcpClient client = new TcpClient("127.0.0.1", port, 1000)) {
            client.connect();

            // 1. Put key (NoSQL) should succeed
            byte[] keyBytes = "k1".getBytes(StandardCharsets.UTF_8);
            byte[] valBytes = "v1".getBytes(StandardCharsets.UTF_8);
            ByteBuffer putPayload = ByteBuffer.allocate(4 + keyBytes.length + 4 + valBytes.length);
            putPayload.putInt(keyBytes.length);
            putPayload.put(keyBytes);
            putPayload.putInt(valBytes.length);
            putPayload.put(valBytes);

            Packet putReq = new Packet(Packet.TYPE_REQ_PUT, 1, 0, putPayload.array());
            Packet putResp = client.send(putReq);
            assertEquals(Packet.TYPE_RESP_SUCCESS, putResp.getType());

            // 2. SQL query should fail
            Packet sqlReq = new Packet(Packet.TYPE_REQ_SQL, 2, 0, "SELECT * FROM t;".getBytes(StandardCharsets.UTF_8));
            Packet sqlResp = client.send(sqlReq);
            assertEquals(Packet.TYPE_RESP_ERROR, sqlResp.getType());
            String errorMsg = new String(sqlResp.getPayload(), StandardCharsets.UTF_8);
            assertTrue(errorMsg.contains("SQL operations are disabled in NoSQL mode"));

        } finally {
            server.stop();
        }
    }

    @Test
    void testSqlModeRestrictions() throws Exception {
        int port = findFreePort();
        HashStorageEngine<String, String> engine = new HashStorageEngine<>(
                new StorageConfig(16, 0.75f, "SQL"),
                new VersionGenerator()
        );

        TcpServer server = new TcpServer("127.0.0.1", port, engine);
        server.registerSqlQueryHandler(sql -> "executed_sql_successfully");
        server.start();

        try (TcpClient client = new TcpClient("127.0.0.1", port, 1000)) {
            client.connect();

            // 1. Put key (NoSQL) should fail
            byte[] keyBytes = "k1".getBytes(StandardCharsets.UTF_8);
            byte[] valBytes = "v1".getBytes(StandardCharsets.UTF_8);
            ByteBuffer putPayload = ByteBuffer.allocate(4 + keyBytes.length + 4 + valBytes.length);
            putPayload.putInt(keyBytes.length);
            putPayload.put(keyBytes);
            putPayload.putInt(valBytes.length);
            putPayload.put(valBytes);

            Packet putReq = new Packet(Packet.TYPE_REQ_PUT, 1, 0, putPayload.array());
            Packet putResp = client.send(putReq);
            assertEquals(Packet.TYPE_RESP_ERROR, putResp.getType());
            String errorMsg = new String(putResp.getPayload(), StandardCharsets.UTF_8);
            assertTrue(errorMsg.contains("NoSQL operations are disabled in SQL mode"));

            // 2. SQL query should succeed
            Packet sqlReq = new Packet(Packet.TYPE_REQ_SQL, 2, 0, "SELECT * FROM t;".getBytes(StandardCharsets.UTF_8));
            Packet sqlResp = client.send(sqlReq);
            assertEquals(Packet.TYPE_RESP_SUCCESS, sqlResp.getType());
            String sqlResult = new String(sqlResp.getPayload(), StandardCharsets.UTF_8);
            assertEquals("executed_sql_successfully", sqlResult);

        } finally {
            server.stop();
        }
    }

    @Test
    void testHybridModeAllowsBoth() throws Exception {
        int port = findFreePort();
        HashStorageEngine<String, String> engine = new HashStorageEngine<>(
                new StorageConfig(16, 0.75f, "HYBRID"),
                new VersionGenerator()
        );

        TcpServer server = new TcpServer("127.0.0.1", port, engine);
        server.registerSqlQueryHandler(sql -> "sql_ok");
        server.start();

        try (TcpClient client = new TcpClient("127.0.0.1", port, 1000)) {
            client.connect();

            // 1. Put key (NoSQL) succeeds
            byte[] keyBytes = "k1".getBytes(StandardCharsets.UTF_8);
            byte[] valBytes = "v1".getBytes(StandardCharsets.UTF_8);
            ByteBuffer putPayload = ByteBuffer.allocate(4 + keyBytes.length + 4 + valBytes.length);
            putPayload.putInt(keyBytes.length);
            putPayload.put(keyBytes);
            putPayload.putInt(valBytes.length);
            putPayload.put(valBytes);

            Packet putReq = new Packet(Packet.TYPE_REQ_PUT, 1, 0, putPayload.array());
            Packet putResp = client.send(putReq);
            assertEquals(Packet.TYPE_RESP_SUCCESS, putResp.getType());

            // 2. SQL query succeeds
            Packet sqlReq = new Packet(Packet.TYPE_REQ_SQL, 2, 0, "SELECT * FROM t;".getBytes(StandardCharsets.UTF_8));
            Packet sqlResp = client.send(sqlReq);
            assertEquals(Packet.TYPE_RESP_SUCCESS, sqlResp.getType());
            String sqlResult = new String(sqlResp.getPayload(), StandardCharsets.UTF_8);
            assertEquals("sql_ok", sqlResult);

        } finally {
            server.stop();
        }
    }
}
