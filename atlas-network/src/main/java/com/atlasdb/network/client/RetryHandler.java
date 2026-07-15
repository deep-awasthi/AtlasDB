package com.atlasdb.network.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.concurrent.Callable;

/**
 * Utility class to execute operations with exponential backoff and jitter retry strategies.
 */
public final class RetryHandler {

    private static final Logger logger = LoggerFactory.getLogger(RetryHandler.class);
    private static final Random random = new Random();

    private final int maxRetries;
    private final long initialDelayMs;
    private final long maxDelayMs;
    private final double multiplier;

    /**
     * Constructs a RetryHandler with default policies:
     * - Max retries: 3
     * - Initial delay: 100ms
     * - Max delay: 2000ms
     * - Multiplier: 2.0
     */
    public RetryHandler() {
        this(3, 100, 2000, 2.0);
    }

    /**
     * Constructs a RetryHandler with custom policies.
     *
     * @param maxRetries     maximum execution attempts
     * @param initialDelayMs starting backoff delay in milliseconds
     * @param maxDelayMs     upper boundary for backoff delays in milliseconds
     * @param multiplier     multiplier rate for exponential scaling
     */
    public RetryHandler(int maxRetries, long initialDelayMs, long maxDelayMs, double multiplier) {
        this.maxRetries = maxRetries;
        this.initialDelayMs = initialDelayMs;
        this.maxDelayMs = maxDelayMs;
        this.multiplier = multiplier;
    }

    /**
     * Executes a callable action, retrying with exponential backoff on exception.
     *
     * @param action the operation to execute
     * @param <T>    the return type
     * @return the result of the action
     * @throws Exception if all retry attempts are exhausted
     */
    public <T> T execute(Callable<T> action) throws Exception {
        int attempts = 0;
        long currentDelay = initialDelayMs;

        while (true) {
            try {
                return action.call();
            } catch (Exception e) {
                attempts++;
                if (attempts > maxRetries) {
                    logger.error("Operation failed after {} attempts.", attempts, e);
                    throw e;
                }

                // Calculate exponential backoff with jitter
                long jitter = (long) (random.nextDouble() * currentDelay * 0.1); // 10% jitter
                long sleepTime = Math.min(currentDelay + jitter, maxDelayMs);

                logger.warn("Operation failed (attempt {}/{}). Retrying in {} ms. Error: {}", 
                        attempts, maxRetries, sleepTime, e.getMessage());

                try {
                    Thread.sleep(sleepTime);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw ie;
                }

                currentDelay = (long) (currentDelay * multiplier);
            }
        }
    }
}
