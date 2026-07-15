package com.atlasdb.common;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Thread-safe generator for monotonically increasing version numbers (timestamps).
 * Primarily used to version modifications and support multi-version concurrency control (MVCC).
 */
public final class VersionGenerator {

    private final AtomicLong version = new AtomicLong(0);

    /**
     * Retrieves the next version number.
     *
     * @return the next monotonic version number (greater than 0)
     */
    public long nextVersion() {
        return version.incrementAndGet();
    }

    /**
     * Retrieves the current version number.
     *
     * @return the current version number
     */
    public long currentVersion() {
        return version.get();
    }

    /**
     * Advances the version sequence to at least the specified target version.
     *
     * @param targetVersion the version value to advance to
     */
    public void advanceTo(long targetVersion) {
        version.accumulateAndGet(targetVersion, Math::max);
    }
}
