package com.atlasdb.network.server;

import com.atlasdb.network.protocol.Packet;
import com.atlasdb.network.protocol.PacketCodec;
import com.atlasdb.storage.StorageEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Non-blocking Java NIO TCP Server for AtlasDB.
 * Leverages a single-threaded selector loop for connection multiplexing
 * and offloads database processing tasks to Project Loom virtual threads.
 */
public final class TcpServer {

    private static final Logger logger = LoggerFactory.getLogger(TcpServer.class);

    private final String host;
    private final int port;
    private final StorageEngine<String, String> storageEngine;
    private final ExecutorService virtualThreadExecutor;
    private final AtomicBoolean running = new AtomicBoolean(false);
    
    private ServerSocketChannel serverChannel;
    private Selector selector;
    private Thread serverThread;

    // Track active connection's last activity timestamp for idle pruning
    private final Map<SocketChannel, Long> activeConnections = new ConcurrentHashMap<>();
    private final ExecutorService janitorExecutor = Executors.newSingleThreadExecutor();

    public interface CustomPacketHandler {
        boolean handle(SocketChannel client, Packet packet) throws IOException;
    }

    public interface SqlQueryHandler {
        String executeQuery(String sql) throws Exception;
    }

    private final java.util.List<CustomPacketHandler> customPacketHandlers = new java.util.concurrent.CopyOnWriteArrayList<>();
    private volatile SqlQueryHandler sqlQueryHandler;

    public void registerHandler(CustomPacketHandler handler) {
        customPacketHandlers.add(java.util.Objects.requireNonNull(handler));
    }

    public void registerSqlQueryHandler(SqlQueryHandler handler) {
        this.sqlQueryHandler = handler;
    }

    /**
     * Constructs a TcpServer.
     *
     * @param host          the binding host address
     * @param port          the listening port
     * @param storageEngine the storage engine to route operations to
     */
    public TcpServer(String host, int port, StorageEngine<String, String> storageEngine) {
        this.host = host;
        this.port = port;
        this.storageEngine = storageEngine;
        this.virtualThreadExecutor = Executors.newVirtualThreadPerTaskExecutor();
    }

    /**
     * Starts the server selector loop.
     *
     * @throws IOException if selector opening or binding fails
     */
    public synchronized void start() throws IOException {
        if (running.getAndSet(true)) {
            return;
        }

        selector = Selector.open();
        serverChannel = ServerSocketChannel.open();
        serverChannel.configureBlocking(false);
        serverChannel.bind(new InetSocketAddress(host, port));
        serverChannel.register(selector, SelectionKey.OP_ACCEPT);

        logger.info("AtlasDB TCP Server started on {}:{}", host, port);

        serverThread = new Thread(this::runLoop, "atlasdb-server-selector");
        serverThread.start();

        // Start idle connection janitor task
        janitorExecutor.submit(this::janitorLoop);
    }

    /**
     * Stops the server, releasing all selectors, channels, and executors.
     */
    public synchronized void stop() {
        if (!running.getAndSet(false)) {
            return;
        }

        logger.info("Stopping AtlasDB TCP Server...");
        if (selector != null) {
            selector.wakeup();
        }

        try {
            if (serverChannel != null) {
                serverChannel.close();
            }
        } catch (IOException e) {
            logger.error("Error closing server channel", e);
        }

        virtualThreadExecutor.shutdown();
        janitorExecutor.shutdownNow();

        for (SocketChannel clientChannel : activeConnections.keySet()) {
            closeChannel(clientChannel);
        }
        activeConnections.clear();

        try {
            if (serverThread != null) {
                serverThread.join(2000);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        try {
            if (selector != null) {
                selector.close();
            }
        } catch (IOException e) {
            logger.error("Error closing selector", e);
        }
        logger.info("AtlasDB TCP Server stopped.");
    }

    private void runLoop() {
        while (running.get()) {
            try {
                int selected = selector.select(1000);
                if (selected == 0) {
                    continue;
                }

                Set<SelectionKey> selectedKeys = selector.selectedKeys();
                Iterator<SelectionKey> keyIterator = selectedKeys.iterator();

                while (keyIterator.hasNext()) {
                    SelectionKey key = keyIterator.next();
                    keyIterator.remove();

                    if (!key.isValid()) {
                        continue;
                    }

                    if (key.isAcceptable()) {
                        handleAccept(key);
                    } else if (key.isReadable()) {
                        handleRead(key);
                    }
                }
            } catch (ClosedSelectorException cse) {
                break;
            } catch (Exception e) {
                if (running.get()) {
                    logger.error("Exception in selector loop", e);
                }
            }
        }
    }

    private void handleAccept(SelectionKey key) {
        try {
            ServerSocketChannel server = (ServerSocketChannel) key.channel();
            SocketChannel client = server.accept();
            if (client == null) {
                return;
            }
            client.configureBlocking(false);
            
            // Attach a session reading buffer (initial capacity: 1KB)
            ByteBuffer readBuffer = ByteBuffer.allocate(1024);
            client.register(selector, SelectionKey.OP_READ, readBuffer);

            activeConnections.put(client, System.currentTimeMillis());
            logger.debug("Accepted connection from {}", client.getRemoteAddress());
        } catch (IOException e) {
            logger.error("Failed to accept client connection", e);
        }
    }

    private void handleRead(SelectionKey key) {
        SocketChannel client = (SocketChannel) key.channel();
        ByteBuffer buffer = (ByteBuffer) key.attachment();

        // Update activity timestamp
        activeConnections.put(client, System.currentTimeMillis());

        try {
            int read = client.read(buffer);
            if (read == -1) {
                closeChannel(client);
                return;
            }

            buffer.flip();
            
            // Parse all complete packets in the buffer
            while (true) {
                Packet packet;
                try {
                    packet = PacketCodec.decode(buffer);
                } catch (Exception e) {
                    logger.error("Protocol decoding error from client {}: {}", client.getRemoteAddress(), e.getMessage());
                    closeChannel(client);
                    return;
                }

                if (packet == null) {
                    // Incomplete packet, need to read more bytes
                    break;
                }

                // Process complete packet in a Loom virtual thread
                virtualThreadExecutor.submit(() -> processPacket(client, packet));
            }

            // Compact the buffer to prepare for the next read event
            buffer.compact();
            
            // Expand buffer if it is completely full and has no space left
            if (buffer.position() == buffer.capacity()) {
                ByteBuffer newBuffer = ByteBuffer.allocate(buffer.capacity() * 2);
                buffer.flip();
                newBuffer.put(buffer);
                key.attach(newBuffer);
            }
        } catch (IOException e) {
            closeChannel(client);
        }
    }

    private String getDbMode() {
        if (storageEngine instanceof com.atlasdb.storage.HashStorageEngine) {
            return ((com.atlasdb.storage.HashStorageEngine<String, String>) storageEngine).getDbMode();
        }
        return "HYBRID";
    }

    private void processPacket(SocketChannel client, Packet packet) {
        try {
            activeConnections.put(client, System.currentTimeMillis());
            
            for (CustomPacketHandler handler : customPacketHandlers) {
                if (handler.handle(client, packet)) {
                    return;
                }
            }
            
            switch (packet.getType()) {
                case Packet.TYPE_HEARTBEAT -> {
                    // Respond with HEARTBEAT ack
                    sendPacket(client, new Packet(Packet.TYPE_HEARTBEAT, packet.getRequestId(), 0, new byte[0]));
                }
                case Packet.TYPE_REQ_PUT -> {
                    String dbMode = getDbMode();
                    if ("SQL".equalsIgnoreCase(dbMode)) {
                        sendPacket(client, new Packet(Packet.TYPE_RESP_ERROR, packet.getRequestId(), 0, "NoSQL operations are disabled in SQL mode.".getBytes(StandardCharsets.UTF_8)));
                        return;
                    }

                    ByteBuffer payload = ByteBuffer.wrap(packet.getPayload());
                    int keyLen = payload.getInt();
                    byte[] keyBytes = new byte[keyLen];
                    payload.get(keyBytes);
                    int valLen = payload.getInt();
                    byte[] valBytes = new byte[valLen];
                    payload.get(valBytes);

                    String key = new String(keyBytes, StandardCharsets.UTF_8);
                    String val = new String(valBytes, StandardCharsets.UTF_8);

                    long version = storageEngine.put(key, val);

                    ByteBuffer respPayload = ByteBuffer.allocate(8);
                    respPayload.putLong(version);
                    respPayload.flip();

                    sendPacket(client, new Packet(Packet.TYPE_RESP_SUCCESS, packet.getRequestId(), 0, respPayload.array()));
                }
                case Packet.TYPE_REQ_GET -> {
                    String dbMode = getDbMode();
                    if ("SQL".equalsIgnoreCase(dbMode)) {
                        sendPacket(client, new Packet(Packet.TYPE_RESP_ERROR, packet.getRequestId(), 0, "NoSQL operations are disabled in SQL mode.".getBytes(StandardCharsets.UTF_8)));
                        return;
                    }

                    String key = new String(packet.getPayload(), StandardCharsets.UTF_8);
                    String val = storageEngine.get(key);

                    if (val == null) {
                        sendPacket(client, new Packet(Packet.TYPE_RESP_ERROR, packet.getRequestId(), 0, "Key not found".getBytes(StandardCharsets.UTF_8)));
                    } else {
                        long version = storageEngine.getVersion(key);
                        byte[] valBytes = val.getBytes(StandardCharsets.UTF_8);
                        ByteBuffer respPayload = ByteBuffer.allocate(8 + 4 + valBytes.length);
                        respPayload.putLong(version);
                        respPayload.putInt(valBytes.length);
                        respPayload.put(valBytes);
                        respPayload.flip();

                        sendPacket(client, new Packet(Packet.TYPE_RESP_SUCCESS, packet.getRequestId(), 0, respPayload.array()));
                    }
                }
                case Packet.TYPE_REQ_DELETE -> {
                    String dbMode = getDbMode();
                    if ("SQL".equalsIgnoreCase(dbMode)) {
                        sendPacket(client, new Packet(Packet.TYPE_RESP_ERROR, packet.getRequestId(), 0, "NoSQL operations are disabled in SQL mode.".getBytes(StandardCharsets.UTF_8)));
                        return;
                    }

                    String key = new String(packet.getPayload(), StandardCharsets.UTF_8);
                    boolean deleted = storageEngine.delete(key);

                    if (deleted) {
                        sendPacket(client, new Packet(Packet.TYPE_RESP_SUCCESS, packet.getRequestId(), 0, new byte[0]));
                    } else {
                        sendPacket(client, new Packet(Packet.TYPE_RESP_ERROR, packet.getRequestId(), 0, "Key not found".getBytes(StandardCharsets.UTF_8)));
                    }
                }
                case Packet.TYPE_REQ_SQL -> {
                    String dbMode = getDbMode();
                    if ("NOSQL".equalsIgnoreCase(dbMode)) {
                        sendPacket(client, new Packet(Packet.TYPE_RESP_ERROR, packet.getRequestId(), 0, "SQL operations are disabled in NoSQL mode.".getBytes(StandardCharsets.UTF_8)));
                        return;
                    }
                    if (sqlQueryHandler == null) {
                        sendPacket(client, new Packet(Packet.TYPE_RESP_ERROR, packet.getRequestId(), 0, "SQL execution handler not registered on server.".getBytes(StandardCharsets.UTF_8)));
                        return;
                    }

                    String sql = new String(packet.getPayload(), StandardCharsets.UTF_8);
                    try {
                        String result = sqlQueryHandler.executeQuery(sql);
                        sendPacket(client, new Packet(Packet.TYPE_RESP_SUCCESS, packet.getRequestId(), 0, result.getBytes(StandardCharsets.UTF_8)));
                    } catch (Exception e) {
                        sendPacket(client, new Packet(Packet.TYPE_RESP_ERROR, packet.getRequestId(), 0, e.getMessage().getBytes(StandardCharsets.UTF_8)));
                    }
                }
                default -> {
                    logger.error("Unknown packet type: {}", packet.getType());
                    sendPacket(client, new Packet(Packet.TYPE_RESP_ERROR, packet.getRequestId(), 0, "Unknown operation".getBytes(StandardCharsets.UTF_8)));
                }
            }
        } catch (Exception e) {
            logger.error("Error executing packet logic in virtual thread", e);
            sendPacket(client, new Packet(Packet.TYPE_RESP_ERROR, packet.getRequestId(), 0, e.getMessage().getBytes(StandardCharsets.UTF_8)));
        }
    }

    public void sendPacket(SocketChannel client, Packet packet) {
        ByteBuffer writeBuffer = PacketCodec.encode(packet);
        synchronized (client) {
            try {
                while (writeBuffer.hasRemaining()) {
                    int written = client.write(writeBuffer);
                    if (written == 0) {
                        // Channel buffer is temporarily full, yield and retry (non-blocking yield)
                        Thread.yield();
                    }
                }
            } catch (IOException e) {
                closeChannel(client);
            }
        }
    }

    private void closeChannel(SocketChannel client) {
        activeConnections.remove(client);
        try {
            logger.debug("Closing channel: {}", client.getRemoteAddress());
            client.close();
        } catch (IOException e) {
            // Ignore
        }
    }

    private void janitorLoop() {
        while (running.get()) {
            try {
                Thread.sleep(5000);
                long now = System.currentTimeMillis();
                long timeout = 30000; // 30 seconds idle timeout

                for (Map.Entry<SocketChannel, Long> entry : activeConnections.entrySet()) {
                    if (now - entry.getValue() > timeout) {
                        logger.warn("Closing connection due to idle timeout: {}", entry.getKey().getRemoteAddress());
                        closeChannel(entry.getKey());
                    }
                }
            } catch (InterruptedException ie) {
                break;
            } catch (Exception e) {
                logger.error("Exception in idle janitor task", e);
            }
        }
    }

    /**
     * Checks if the server is currently running.
     *
     * @return true if running, false otherwise
     */
    public boolean isRunning() {
        return running.get();
    }
}
