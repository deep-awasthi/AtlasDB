package com.atlasdb.network.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A thread-safe connection pool for managing reusable TcpClient instances.
 */
public final class ConnectionPool implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(ConnectionPool.class);

    private final String host;
    private final int port;
    private final int timeoutMs;
    private final int maxConnections;
    
    private final BlockingQueue<TcpClient> pool;
    private final AtomicInteger activeConnectionsCount = new AtomicInteger(0);

    /**
     * Constructs a ConnectionPool.
     *
     * @param host           the database server host
     * @param port           the database server port
     * @param timeoutMs      socket timeout in milliseconds
     * @param maxConnections maximum connections to hold in the pool
     */
    public ConnectionPool(String host, int port, int timeoutMs, int maxConnections) {
        this.host = host;
        this.port = port;
        this.timeoutMs = timeoutMs;
        this.maxConnections = maxConnections;
        this.pool = new LinkedBlockingQueue<>(maxConnections);
    }

    /**
     * Borrows a connection from the pool.
     * Blocks if the maximum number of connections has been reached and none are available.
     *
     * @param borrowTimeoutMs timeout in milliseconds to wait for a connection
     * @return a connected TcpClient
     * @throws IOException if connection fails or wait times out
     */
    public TcpClient borrowConnection(long borrowTimeoutMs) throws IOException {
        // Try to get an idle connection from the pool first
        TcpClient client = pool.poll();
        if (client != null) {
            if (client.isConnected()) {
                return client;
            } else {
                activeConnectionsCount.decrementAndGet();
                client.close();
            }
        }

        // If no idle connection, try to spin up a new one if limit not reached
        while (true) {
            int currentCount = activeConnectionsCount.get();
            if (currentCount >= maxConnections) {
                break;
            }
            if (activeConnectionsCount.compareAndSet(currentCount, currentCount + 1)) {
                try {
                    TcpClient newClient = new TcpClient(host, port, timeoutMs);
                    newClient.connect();
                    return newClient;
                } catch (IOException e) {
                    activeConnectionsCount.decrementAndGet();
                    throw e;
                }
            }
        }

        // Wait for an active connection to be returned
        try {
            client = pool.poll(borrowTimeoutMs, TimeUnit.MILLISECONDS);
            if (client == null) {
                throw new IOException("Timeout borrowing connection from pool.");
            }
            if (!client.isConnected()) {
                activeConnectionsCount.decrementAndGet();
                client.close();
                return borrowConnection(borrowTimeoutMs); // Try again
            }
            return client;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while borrowing connection from pool.", e);
        }
    }

    /**
     * Returns a connection to the pool.
     * If the connection is broken or closed, it is destroyed instead of returned.
     *
     * @param client the client connection to return
     */
    public void returnConnection(TcpClient client) {
        if (client == null) {
            return;
        }

        if (client.isConnected()) {
            boolean added = pool.offer(client);
            if (!added) {
                // Pool is full, destroy connection
                activeConnectionsCount.decrementAndGet();
                client.close();
            }
        } else {
            activeConnectionsCount.decrementAndGet();
            client.close();
        }
    }

    /**
     * Retrieves the current count of active connections.
     *
     * @return count of active connections
     */
    public int getActiveConnectionsCount() {
        return activeConnectionsCount.get();
    }

    /**
     * Retrieves the number of idle connections currently in the pool queue.
     *
     * @return count of idle connections
     */
    public int getIdleConnectionsCount() {
        return pool.size();
    }

    @Override
    public void close() {
        logger.info("Closing connection pool...");
        TcpClient client;
        while ((client = pool.poll()) != null) {
            client.close();
        }
        activeConnectionsCount.set(0);
    }
}
