package com.atlasdb.network;

import com.atlasdb.common.VersionGenerator;
import com.atlasdb.network.client.ConnectionPool;
import com.atlasdb.network.client.RetryHandler;
import com.atlasdb.network.client.TcpClient;
import com.atlasdb.network.protocol.Packet;
import com.atlasdb.network.protocol.PacketCodec;
import com.atlasdb.network.server.TcpServer;
import com.atlasdb.storage.HashStorageEngine;
import com.atlasdb.storage.StorageEngine;
import com.atlasdb.storage.config.StorageConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class NetworkIntegrationTest {

    private TcpServer server;
    private StorageEngine<String, String> storageEngine;
    private int port;
    private String host = "127.0.0.1";

    private static int findFreePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    @BeforeEach
    void setUp() throws IOException {
        port = findFreePort();
        storageEngine = new HashStorageEngine<>(
                new StorageConfig(16, 0.75f),
                new VersionGenerator()
        );
        server = new TcpServer(host, port, storageEngine);
        server.start();
    }

    @AfterEach
    void tearDown() {
        if (server != null) {
            server.stop();
        }
    }

    @Test
    void testSingleClientPutGetDelete() throws Exception {
        try (TcpClient client = new TcpClient(host, port, 1000)) {
            client.connect();

            // 1. Put k1 = v1
            byte[] keyBytes = "k1".getBytes(StandardCharsets.UTF_8);
            byte[] valBytes = "v1".getBytes(StandardCharsets.UTF_8);
            ByteBuffer putPayload = ByteBuffer.allocate(4 + keyBytes.length + 4 + valBytes.length);
            putPayload.putInt(keyBytes.length);
            putPayload.put(keyBytes);
            putPayload.putInt(valBytes.length);
            putPayload.put(valBytes);

            Packet putReq = new Packet(Packet.TYPE_REQ_PUT, 101, 0, putPayload.array());
            Packet putResp = client.send(putReq);

            assertEquals(Packet.TYPE_RESP_SUCCESS, putResp.getType());
            assertEquals(101, putResp.getRequestId());
            ByteBuffer putRespBuffer = ByteBuffer.wrap(putResp.getPayload());
            long version = putRespBuffer.getLong();
            assertTrue(version > 0);

            // 2. Get k1
            Packet getReq = new Packet(Packet.TYPE_REQ_GET, 102, 0, "k1".getBytes(StandardCharsets.UTF_8));
            Packet getResp = client.send(getReq);

            assertEquals(Packet.TYPE_RESP_SUCCESS, getResp.getType());
            assertEquals(102, getResp.getRequestId());
            ByteBuffer getRespBuffer = ByteBuffer.wrap(getResp.getPayload());
            long getVersion = getRespBuffer.getLong();
            int getValLen = getRespBuffer.getInt();
            byte[] getValBytes = new byte[getValLen];
            getRespBuffer.get(getValBytes);

            assertEquals(version, getVersion);
            assertEquals("v1", new String(getValBytes, StandardCharsets.UTF_8));

            // 3. Delete k1
            Packet delReq = new Packet(Packet.TYPE_REQ_DELETE, 103, 0, "k1".getBytes(StandardCharsets.UTF_8));
            Packet delResp = client.send(delReq);
            assertEquals(Packet.TYPE_RESP_SUCCESS, delResp.getType());

            // 4. Verify k1 is deleted
            Packet getReq2 = new Packet(Packet.TYPE_REQ_GET, 104, 0, "k1".getBytes(StandardCharsets.UTF_8));
            Packet getResp2 = client.send(getReq2);
            assertEquals(Packet.TYPE_RESP_ERROR, getResp2.getType());
            assertEquals("Key not found", new String(getResp2.getPayload(), StandardCharsets.UTF_8));
        }
    }

    @Test
    void testHeartbeat() throws Exception {
        try (TcpClient client = new TcpClient(host, port, 1000)) {
            client.connect();

            Packet heartbeat = new Packet(Packet.TYPE_HEARTBEAT, 999, 0, new byte[0]);
            Packet heartbeatAck = client.send(heartbeat);

            assertEquals(Packet.TYPE_HEARTBEAT, heartbeatAck.getType());
            assertEquals(999, heartbeatAck.getRequestId());
        }
    }

    @Test
    void testConnectionPoolConcurrency() throws Exception {
        int poolSize = 5;
        int clientThreads = 10;
        int operationsPerThread = 100;

        try (ConnectionPool connectionPool = new ConnectionPool(host, port, 1000, poolSize)) {
            ExecutorService executor = Executors.newFixedThreadPool(clientThreads);
            CountDownLatch latch = new CountDownLatch(1);
            List<Future<Void>> futures = new ArrayList<>();

            for (int t = 0; t < clientThreads; t++) {
                final int threadId = t;
                futures.add(executor.submit(() -> {
                    latch.await();
                    for (int i = 0; i < operationsPerThread; i++) {
                        String key = "key-" + threadId + "-" + i;
                        String val = "val-" + i;

                        // Borrow
                        TcpClient client = connectionPool.borrowConnection(2000);
                        try {
                            // Construct PUT
                            byte[] keyB = key.getBytes(StandardCharsets.UTF_8);
                            byte[] valB = val.getBytes(StandardCharsets.UTF_8);
                            ByteBuffer putPayload = ByteBuffer.allocate(4 + keyB.length + 4 + valB.length);
                            putPayload.putInt(keyB.length);
                            putPayload.put(keyB);
                            putPayload.putInt(valB.length);
                            putPayload.put(valB);

                            Packet response = client.send(new Packet(Packet.TYPE_REQ_PUT, i, 0, putPayload.array()));
                            assertEquals(Packet.TYPE_RESP_SUCCESS, response.getType());
                        } finally {
                            // Return
                            connectionPool.returnConnection(client);
                        }
                    }
                    return null;
                }));
            }

            latch.countDown(); // Release threads
            for (Future<Void> f : futures) {
                f.get(10, TimeUnit.SECONDS);
            }

            executor.shutdown();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));

            // Verify stored size
            assertEquals(clientThreads * operationsPerThread, storageEngine.size());
        }
    }

    @Test
    void testRetryHandler() throws Exception {
        RetryHandler retryHandler = new RetryHandler(2, 50, 200, 2.0);

        AtomicInteger attempts = new AtomicInteger(0);
        String result = retryHandler.execute(() -> {
            int current = attempts.incrementAndGet();
            if (current < 3) {
                throw new IOException("Simulated network failure");
            }
            return "success-on-third-attempt";
        });

        assertEquals("success-on-third-attempt", result);
        assertEquals(3, attempts.get());
    }

    @Test
    void testProtocolInvalidMagic() throws Exception {
        try (TcpClient client = new TcpClient(host, port, 1000)) {
            client.connect();

            // Create a packet but corrupt the magic byte manually
            Packet validPacket = new Packet(Packet.TYPE_HEARTBEAT, 1, 0, new byte[0]);
            ByteBuffer rawBuffer = PacketCodec.encode(validPacket);
            rawBuffer.put(0, (byte) 0x99); // Corrupt magic byte

            // Send corrupted bytes manually
            java.nio.channels.SocketChannel socketChannel = java.nio.channels.SocketChannel.open(new java.net.InetSocketAddress(host, port));
            socketChannel.write(rawBuffer);

            // Read response: Server should close connection due to protocol violation
            ByteBuffer responseBuffer = ByteBuffer.allocate(100);
            int bytesRead = socketChannel.read(responseBuffer);
            
            // Channel should indicate EOF (-1)
            assertEquals(-1, bytesRead);
            socketChannel.close();
        }
    }
}
