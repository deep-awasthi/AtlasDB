package com.atlasdb.benchmark;

import com.atlasdb.common.VersionGenerator;
import com.atlasdb.storage.HashStorageEngine;
import com.atlasdb.storage.StorageEngine;
import com.atlasdb.storage.config.StorageConfig;
import org.openjdk.jmh.annotations.*;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * JMH Benchmark to evaluate the read and write throughput of HashStorageEngine.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 2, time = 1)
@Measurement(iterations = 3, time = 1)
@Fork(1)
public class StorageEngineBenchmark {

    private StorageEngine<String, String> engine;
    private final AtomicInteger counter = new AtomicInteger(0);

    /**
     * Initializes the storage engine with pre-populated values.
     */
    @Setup
    public void setup() {
        StorageConfig config = new StorageConfig(16384, 0.75f);
        VersionGenerator versionGenerator = new VersionGenerator();
        engine = new HashStorageEngine<>(config, versionGenerator);

        for (int i = 0; i < 10000; i++) {
            engine.put("key-" + i, "value-" + i);
        }
    }

    /**
     * Measures get throughput for pre-existing keys.
     *
     * @return the retrieved value
     */
    @Benchmark
    @Group("read_write")
    @GroupThreads(4)
    public String testGet() {
        // Read keys in a cyclic manner to avoid absolute cache hit bias
        int idx = (counter.getAndIncrement() & 0x7fffffff) % 10000;
        return engine.get("key-" + idx);
    }

    /**
     * Measures put throughput.
     *
     * @return the write version
     */
    @Benchmark
    @Group("read_write")
    @GroupThreads(2)
    public long testPut() {
        int idx = (counter.getAndIncrement() & 0x7fffffff) % 10000;
        return engine.put("key-" + idx, "new-value-" + idx);
    }
}
