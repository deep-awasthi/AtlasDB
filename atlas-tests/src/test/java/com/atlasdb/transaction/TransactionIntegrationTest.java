package com.atlasdb.transaction;

import com.atlasdb.common.VersionGenerator;
import com.atlasdb.storage.HashStorageEngine;
import com.atlasdb.storage.TimestampManager;
import com.atlasdb.storage.config.StorageConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class TransactionIntegrationTest {

    private HashStorageEngine<String, String> engine;
    private TimestampManager timestampManager;
    private TransactionManager transactionManager;

    @BeforeEach
    void setUp() {
        VersionGenerator versionGenerator = new VersionGenerator();
        engine = new HashStorageEngine<>(
                new StorageConfig(8, 0.75f),
                versionGenerator
        );
        timestampManager = new TimestampManager(versionGenerator);
        transactionManager = new TransactionManager(engine, timestampManager);
    }

    @AfterEach
    void tearDown() {
        if (transactionManager != null) {
            transactionManager.close();
        }
    }

    @Test
    void testSnapshotIsolationVisibility() {
        engine.put("k1", "v1");

        Transaction txn1 = transactionManager.begin(
                Transaction.IsolationLevel.SNAPSHOT_ISOLATION,
                Transaction.ConcurrencyMode.OPTIMISTIC
        );
        
        Transaction txn2 = transactionManager.begin(
                Transaction.IsolationLevel.SNAPSHOT_ISOLATION,
                Transaction.ConcurrencyMode.OPTIMISTIC
        );

        // txn2 updates and commits
        txn2.put("k1", "v2");
        txn2.commit();

        // txn1 should still read v1 (repeatable snapshot read)
        assertEquals("v1", txn1.get("k1"));
        txn1.commit();

        // New transaction after commit should see v2
        Transaction txn3 = transactionManager.begin(
                Transaction.IsolationLevel.SNAPSHOT_ISOLATION,
                Transaction.ConcurrencyMode.OPTIMISTIC
        );
        assertEquals("v2", txn3.get("k1"));
        txn3.commit();
    }

    @Test
    void testReadCommittedVisibility() {
        engine.put("k1", "v1");

        Transaction txn1 = transactionManager.begin(
                Transaction.IsolationLevel.READ_COMMITTED,
                Transaction.ConcurrencyMode.OPTIMISTIC
        );

        Transaction txn2 = transactionManager.begin(
                Transaction.IsolationLevel.READ_COMMITTED,
                Transaction.ConcurrencyMode.OPTIMISTIC
        );

        // txn1 reads v1 initially
        assertEquals("v1", txn1.get("k1"));

        // txn2 updates and commits v2
        txn2.put("k1", "v2");
        txn2.commit();

        // txn1 reads again under READ_COMMITTED, should see the new committed value v2
        assertEquals("v2", txn1.get("k1"));
        txn1.commit();
    }

    @Test
    void testOccConflictValidationAndAbort() {
        engine.put("k1", "v1");

        Transaction txn1 = transactionManager.begin(
                Transaction.IsolationLevel.SNAPSHOT_ISOLATION,
                Transaction.ConcurrencyMode.OPTIMISTIC
        );

        Transaction txn2 = transactionManager.begin(
                Transaction.IsolationLevel.SNAPSHOT_ISOLATION,
                Transaction.ConcurrencyMode.OPTIMISTIC
        );

        // txn1 reads k1
        txn1.get("k1");

        // txn2 writes k1 and commits (advancing k1's version)
        txn2.put("k1", "v2");
        txn2.commit();

        // txn1 tries to write k1 and commit (OCC conflict on k1 read)
        txn1.put("k1", "v3");
        
        Exception ex = assertThrows(IllegalStateException.class, txn1::commit);
        assertTrue(ex.getMessage().contains("OCC Validation failed"));
        assertEquals(Transaction.State.ABORTED, txn1.getState());
    }

    @Test
    void testPessimisticLockBlocking() throws InterruptedException, ExecutionException {
        engine.put("k1", "v1");

        Transaction txn1 = transactionManager.begin(
                Transaction.IsolationLevel.SNAPSHOT_ISOLATION,
                Transaction.ConcurrencyMode.PESSIMISTIC
        );

        // txn1 writes k1, holding the write lock
        txn1.put("k1", "v1-updated");

        AtomicBoolean threadStarted = new AtomicBoolean(false);
        AtomicBoolean lockAcquired = new AtomicBoolean(false);
        CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
            threadStarted.set(true);
            Transaction txn2 = transactionManager.begin(
                    Transaction.IsolationLevel.SNAPSHOT_ISOLATION,
                    Transaction.ConcurrencyMode.PESSIMISTIC
            );
            // This put will block until txn1 releases the write lock on k1
            txn2.put("k1", "v1-txn2");
            lockAcquired.set(true);
            txn2.commit();
        });

        // Let the background thread start and block
        Thread.sleep(100);
        assertTrue(threadStarted.get());
        assertFalse(lockAcquired.get()); // Should block

        // Commit txn1, releasing its locks
        txn1.commit();

        // Wait for txn2 thread to finish
        future.get();
        assertTrue(lockAcquired.get()); // Should unblock and acquire lock

        // Verify result in database
        assertEquals("v1-txn2", engine.get("k1"));
    }

    @Test
    void testDeadlockDetectionAndResolution() throws InterruptedException {
        // Prepare keys
        engine.put("k1", "v1");
        engine.put("k2", "v2");

        Transaction txn1 = transactionManager.begin(
                Transaction.IsolationLevel.SNAPSHOT_ISOLATION,
                Transaction.ConcurrencyMode.PESSIMISTIC
        );

        Transaction txn2 = transactionManager.begin(
                Transaction.IsolationLevel.SNAPSHOT_ISOLATION,
                Transaction.ConcurrencyMode.PESSIMISTIC
        );

        // txn1 locks k1
        txn1.put("k1", "txn1-write");

        // txn2 locks k2
        txn2.put("k2", "txn2-write");

        // Launch txn1 attempting to lock k2 in a virtual thread
        AtomicReference<Exception> txn1Exception = new AtomicReference<>();
        Thread t1 = Thread.ofVirtual().start(() -> {
            try {
                txn1.put("k2", "txn1-deadlock-attempt");
                txn1.commit();
            } catch (Exception e) {
                txn1Exception.set(e);
            }
        });

        // Launch txn2 attempting to lock k1 in a virtual thread
        AtomicReference<Exception> txn2Exception = new AtomicReference<>();
        Thread t2 = Thread.ofVirtual().start(() -> {
            try {
                txn2.put("k1", "txn2-deadlock-attempt");
                txn2.commit();
            } catch (Exception e) {
                txn2Exception.set(e);
            }
        });

        // Wait for deadlock detector to execute (interval is 200ms)
        t1.join(2000);
        t2.join(2000);

        // At least one transaction should fail due to abort by deadlock detector
        boolean txn1Failed = txn1.getState() == Transaction.State.ABORTED || txn1Exception.get() != null;
        boolean txn2Failed = txn2.getState() == Transaction.State.ABORTED || txn2Exception.get() != null;

        assertTrue(txn1Failed || txn2Failed, "At least one transaction should have been aborted to break the deadlock");
    }
}
