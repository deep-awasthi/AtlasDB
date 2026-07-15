package com.atlasdb.monitoring.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Timer;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * Tracks and exposes performance metrics using Micrometer Core and Prometheus configurations.
 */
public final class MetricsManager {

    private final PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);

    private final Counter committedTransactions;
    private final Counter abortedTransactions;
    private final Counter networkRequests;
    private final Timer requestLatency;
    private final AtomicLong activeTransactions = new AtomicLong(0);

    public MetricsManager() {
        this.committedTransactions = Counter.builder("atlasdb.transactions.committed")
                .description("Total committed transactions")
                .register(registry);

        this.abortedTransactions = Counter.builder("atlasdb.transactions.aborted")
                .description("Total aborted transactions")
                .register(registry);

        this.networkRequests = Counter.builder("atlasdb.network.requests.total")
                .description("Total received network requests")
                .register(registry);

        this.requestLatency = Timer.builder("atlasdb.network.requests.duration")
                .description("Execution latency of packet requests")
                .register(registry);

        Gauge.builder("atlasdb.transactions.active", activeTransactions, AtomicLong::get)
                .description("Active concurrent running transactions")
                .register(registry);
    }

    public PrometheusMeterRegistry getRegistry() {
        return registry;
    }

    /**
     * Registers a gauge bound to the active database size.
     */
    public void registerDatabaseSizeGauge(Supplier<Number> databaseSizeSupplier) {
        Gauge.builder("atlasdb.database.size", databaseSizeSupplier)
                .description("Active records size in the storage engine")
                .register(registry);
    }

    public void incrementCommittedTransactions() {
        committedTransactions.increment();
    }

    public void incrementAbortedTransactions() {
        abortedTransactions.increment();
    }

    public void setActiveTransactions(long val) {
        activeTransactions.set(val);
    }

    public void incrementRequests() {
        networkRequests.increment();
    }

    public void recordRequestLatency(long durationMs) {
        requestLatency.record(durationMs, TimeUnit.MILLISECONDS);
    }

    public long getCommittedTransactions() {
        return (long) committedTransactions.count();
    }

    public long getAbortedTransactions() {
        return (long) abortedTransactions.count();
    }

    public long getActiveTransactions() {
        return activeTransactions.get();
    }

    public long getNetworkRequests() {
        return (long) networkRequests.count();
    }
}
