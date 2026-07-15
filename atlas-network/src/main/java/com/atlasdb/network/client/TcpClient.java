package com.atlasdb.network.client;

import com.atlasdb.network.protocol.Packet;
import com.atlasdb.network.protocol.PacketCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

/**
 * TCP Client implementation for AtlasDB.
 * Manages SocketChannel connection and provides synchronous request-response roundtrips.
 */
public final class TcpClient implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(TcpClient.class);

    private final String host;
    private final int port;
    private final int timeoutMs;
    private SocketChannel channel;
    private ByteBuffer readBuffer;

    /**
     * Constructs a TcpClient.
     *
     * @param host      the server host
     * @param port      the server port
     * @param timeoutMs socket timeout in milliseconds
     */
    public TcpClient(String host, int port, int timeoutMs) {
        this.host = host;
        this.port = port;
        this.timeoutMs = timeoutMs;
        this.readBuffer = ByteBuffer.allocate(8192);
    }

    /**
     * Connects to the target server.
     *
     * @throws IOException if connection fails
     */
    public synchronized void connect() throws IOException {
        if (isConnected()) {
            return;
        }

        channel = SocketChannel.open();
        // Keep it blocking for simple synchronous execution in pooled clients
        channel.configureBlocking(true);
        channel.socket().setSoTimeout(timeoutMs);
        channel.socket().setKeepAlive(true);
        channel.connect(new InetSocketAddress(host, port));
        readBuffer.clear();

        logger.debug("Connected to AtlasDB server at {}:{}", host, port);
    }

    /**
     * Sends a packet to the server and blocks until the corresponding response is received.
     *
     * @param packet the request packet to send
     * @return the response packet
     * @throws IOException if network transmission or read fails
     */
    public synchronized Packet send(Packet packet) throws IOException {
        if (!isConnected()) {
            connect();
        }

        // Encode and write the request packet
        ByteBuffer writeBuffer = PacketCodec.encode(packet);
        while (writeBuffer.hasRemaining()) {
            channel.write(writeBuffer);
        }

        // Read and parse response packet
        while (true) {
            readBuffer.flip();
            Packet response = PacketCodec.decode(readBuffer);
            if (response != null) {
                readBuffer.compact();
                return response;
            }
            readBuffer.compact();

            // Prepare buffer to read more bytes
            int limit = readBuffer.limit();
            int pos = readBuffer.position();
            if (pos == limit) {
                // Resize buffer if it is full
                ByteBuffer newBuffer = ByteBuffer.allocate(readBuffer.capacity() * 2);
                readBuffer.flip();
                newBuffer.put(readBuffer);
                readBuffer = newBuffer;
            }

            // Read next chunk from network
            int read = channel.read(readBuffer);
            if (read == -1) {
                close();
                throw new IOException("Connection closed by server while waiting for response.");
            }
        }
    }

    /**
     * Returns true if the client is connected.
     *
     * @return connection state
     */
    public synchronized boolean isConnected() {
        return channel != null && channel.isConnected() && channel.isOpen();
    }

    /**
     * Closes the client connection.
     */
    @Override
    public synchronized void close() {
        if (channel != null) {
            try {
                logger.debug("Closing client connection to {}:{}", host, port);
                channel.close();
            } catch (IOException e) {
                // Ignore
            } finally {
                channel = null;
            }
        }
    }
}
