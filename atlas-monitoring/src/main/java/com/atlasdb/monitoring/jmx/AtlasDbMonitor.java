package com.atlasdb.monitoring.jmx;

import com.atlasdb.monitoring.metrics.MetricsManager;
import com.atlasdb.storage.Entry;
import com.atlasdb.storage.HashStorageEngine;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * Concrete implementation of standard MBean for JMX.
 */
public final class AtlasDbMonitor implements AtlasDbMonitorMBean {

    private final HashStorageEngine<String, String> engine;
    private final MetricsManager metricsManager;

    public AtlasDbMonitor(HashStorageEngine<String, String> engine, MetricsManager metricsManager) {
        this.engine = Objects.requireNonNull(engine, "engine cannot be null");
        this.metricsManager = Objects.requireNonNull(metricsManager, "metricsManager cannot be null");
    }

    @Override
    public int getDatabaseSize() {
        return engine.size();
    }

    @Override
    public long getActiveTransactions() {
        return metricsManager.getActiveTransactions();
    }

    @Override
    public long getCommittedTransactions() {
        return metricsManager.getCommittedTransactions();
    }

    @Override
    public long getAbortedTransactions() {
        return metricsManager.getAbortedTransactions();
    }

    @Override
    public void triggerGarbageCollection() {
        List<String> keys = new ArrayList<>();
        Iterator<Entry<String, String>> it = engine.iterator();
        while (it.hasNext()) {
            keys.add(it.next().getKey());
        }

        long gcTimestamp = System.currentTimeMillis();
        for (String key : keys) {
            engine.pruneKey(key, gcTimestamp);
        }
    }
}
