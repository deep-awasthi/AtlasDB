package com.atlasdb.sql;

import com.atlasdb.common.VersionGenerator;
import com.atlasdb.storage.HashStorageEngine;
import com.atlasdb.storage.TimestampManager;
import com.atlasdb.storage.config.StorageConfig;
import com.atlasdb.sql.catalog.CatalogManager;
import com.atlasdb.sql.executor.ResultSet;
import com.atlasdb.sql.executor.SqlExecutor;
import com.atlasdb.transaction.Transaction;
import com.atlasdb.transaction.TransactionManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class SqlIntegrationTest {

    private HashStorageEngine<String, String> engine;
    private TimestampManager timestampManager;
    private TransactionManager transactionManager;
    private CatalogManager catalogManager;
    private SqlExecutor sqlExecutor;

    @BeforeEach
    void setUp() {
        VersionGenerator versionGenerator = new VersionGenerator();
        engine = new HashStorageEngine<>(
                new StorageConfig(8, 0.75f),
                versionGenerator
        );
        timestampManager = new TimestampManager(versionGenerator);
        transactionManager = new TransactionManager(engine, timestampManager);
        catalogManager = new CatalogManager();
        sqlExecutor = new SqlExecutor(catalogManager);
    }

    @AfterEach
    void tearDown() {
        if (transactionManager != null) {
            transactionManager.close();
        }
    }

    @Test
    void testSqlDdlAndDmlLifecycle() {
        // 1. CREATE TABLE
        Transaction txn1 = transactionManager.begin(
                Transaction.IsolationLevel.SNAPSHOT_ISOLATION,
                Transaction.ConcurrencyMode.OPTIMISTIC
        );
        sqlExecutor.execute(txn1, "CREATE TABLE users (id INT, name VARCHAR, city VARCHAR);");
        txn1.commit();

        // 2. INSERT VALUES
        Transaction txn2 = transactionManager.begin(
                Transaction.IsolationLevel.SNAPSHOT_ISOLATION,
                Transaction.ConcurrencyMode.OPTIMISTIC
        );
        sqlExecutor.execute(txn2, "INSERT INTO users (id, name, city) VALUES (1, 'Alice', 'Boston')");
        sqlExecutor.execute(txn2, "INSERT INTO users (id, name, city) VALUES (2, 'Bob', 'NewYork')");
        txn2.commit();

        // 3. SELECT ALL
        Transaction txn3 = transactionManager.begin(
                Transaction.IsolationLevel.SNAPSHOT_ISOLATION,
                Transaction.ConcurrencyMode.OPTIMISTIC
        );
        ResultSet resAll = sqlExecutor.execute(txn3, "SELECT * FROM users;");
        assertEquals(2, resAll.getRowCount());
        assertEquals(3, resAll.getColumnCount());
        
        // Assert field lookups
        int aliceIdx = resAll.getValue(0, "id").equals("1") ? 0 : 1;
        int bobIdx = 1 - aliceIdx;

        assertEquals("1", resAll.getValue(aliceIdx, "id"));
        assertEquals("Alice", resAll.getValue(aliceIdx, "name"));
        assertEquals("Boston", resAll.getValue(aliceIdx, "city"));
        
        assertEquals("2", resAll.getValue(bobIdx, "id"));
        assertEquals("Bob", resAll.getValue(bobIdx, "name"));
        assertEquals("NewYork", resAll.getValue(bobIdx, "city"));
        txn3.commit();

        // 4. SELECT Projection & Filters
        Transaction txn4 = transactionManager.begin(
                Transaction.IsolationLevel.SNAPSHOT_ISOLATION,
                Transaction.ConcurrencyMode.OPTIMISTIC
        );
        ResultSet resFilter = sqlExecutor.execute(txn4, "SELECT name, city FROM users WHERE id = 2");
        assertEquals(1, resFilter.getRowCount());
        assertEquals(2, resFilter.getColumnCount());
        assertEquals("Bob", resFilter.getValue(0, "name"));
        assertEquals("NewYork", resFilter.getValue(0, "city"));
        txn4.commit();
    }

    @Test
    void testSqlUpdateAndDelete() {
        // Setup schema and initial values
        Transaction txnInit = transactionManager.begin(
                Transaction.IsolationLevel.SNAPSHOT_ISOLATION,
                Transaction.ConcurrencyMode.OPTIMISTIC
        );
        sqlExecutor.execute(txnInit, "CREATE TABLE users (id INT, name VARCHAR, city VARCHAR)");
        sqlExecutor.execute(txnInit, "INSERT INTO users (id, name, city) VALUES (1, 'Alice', 'Boston')");
        sqlExecutor.execute(txnInit, "INSERT INTO users (id, name, city) VALUES (2, 'Bob', 'NewYork')");
        txnInit.commit();

        // Perform UPDATE
        Transaction txnUpdate = transactionManager.begin(
                Transaction.IsolationLevel.SNAPSHOT_ISOLATION,
                Transaction.ConcurrencyMode.OPTIMISTIC
        );
        sqlExecutor.execute(txnUpdate, "UPDATE users SET name = 'BobUpdated', city = 'Chicago' WHERE id = 2");
        txnUpdate.commit();

        // Verify UPDATE
        Transaction txnVerify = transactionManager.begin(
                Transaction.IsolationLevel.SNAPSHOT_ISOLATION,
                Transaction.ConcurrencyMode.OPTIMISTIC
        );
        ResultSet res1 = sqlExecutor.execute(txnVerify, "SELECT name, city FROM users WHERE id = 2");
        assertEquals("BobUpdated", res1.getValue(0, "name"));
        assertEquals("Chicago", res1.getValue(0, "city"));

        // Perform DELETE
        sqlExecutor.execute(txnVerify, "DELETE FROM users WHERE id = 1");
        
        // Verify DELETE
        ResultSet res2 = sqlExecutor.execute(txnVerify, "SELECT * FROM users");
        assertEquals(1, res2.getRowCount());
        assertEquals("BobUpdated", res2.getValue(0, "name"));
        txnVerify.commit();
    }

    @Test
    void testSqlNestedLoopJoin() {
        Transaction txn = transactionManager.begin(
                Transaction.IsolationLevel.SNAPSHOT_ISOLATION,
                Transaction.ConcurrencyMode.OPTIMISTIC
        );
        sqlExecutor.execute(txn, "CREATE TABLE users (id INT, name VARCHAR)");
        sqlExecutor.execute(txn, "CREATE TABLE orders (id INT, user_id INT, amount INT)");

        sqlExecutor.execute(txn, "INSERT INTO users (id, name) VALUES (1, 'Alice')");
        sqlExecutor.execute(txn, "INSERT INTO users (id, name) VALUES (2, 'Bob')");

        sqlExecutor.execute(txn, "INSERT INTO orders (id, user_id, amount) VALUES (101, 2, 100)");
        sqlExecutor.execute(txn, "INSERT INTO orders (id, user_id, amount) VALUES (102, 1, 350)");
        sqlExecutor.execute(txn, "INSERT INTO orders (id, user_id, amount) VALUES (103, 99, 500)"); // non-matching user_id

        // Execute Inner Join: SELECT users.name, orders.amount FROM users JOIN orders ON users.id = orders.user_id
        ResultSet resJoin = sqlExecutor.execute(txn, 
                "SELECT users.name, orders.amount FROM users JOIN orders ON users.id = orders.user_id");

        assertEquals(2, resJoin.getRowCount());
        
        int aliceIdx = resJoin.getValue(0, "users.name").equals("Alice") ? 0 : 1;
        int bobIdx = 1 - aliceIdx;

        assertEquals("Alice", resJoin.getValue(aliceIdx, "users.name"));
        assertEquals("350", resJoin.getValue(aliceIdx, "orders.amount"));

        assertEquals("Bob", resJoin.getValue(bobIdx, "users.name"));
        assertEquals("100", resJoin.getValue(bobIdx, "orders.amount"));

        txn.commit();
    }

    @Test
    void testSqlAggregationsAndGroupBy() {
        Transaction txn = transactionManager.begin(
                Transaction.IsolationLevel.SNAPSHOT_ISOLATION,
                Transaction.ConcurrencyMode.OPTIMISTIC
        );
        sqlExecutor.execute(txn, "CREATE TABLE orders (id INT, user_id INT, amount INT)");

        sqlExecutor.execute(txn, "INSERT INTO orders (id, user_id, amount) VALUES (1, 2, 100)");
        sqlExecutor.execute(txn, "INSERT INTO orders (id, user_id, amount) VALUES (2, 2, 200)");
        sqlExecutor.execute(txn, "INSERT INTO orders (id, user_id, amount) VALUES (3, 3, 400)");

        // Group By query: SELECT user_id, count(*), sum(amount) FROM orders GROUP BY user_id
        ResultSet res = sqlExecutor.execute(txn, 
                "SELECT user_id, count(*), sum(amount) FROM orders GROUP BY user_id");

        assertEquals(2, res.getRowCount());

        int row2Idx = res.getValue(0, "user_id").equals("2") ? 0 : 1;
        int row3Idx = 1 - row2Idx;

        assertEquals("2", res.getValue(row2Idx, "user_id"));
        assertEquals("2", res.getValue(row2Idx, "count(*)"));
        assertEquals("300.0", res.getValue(row2Idx, "sum(amount)"));

        assertEquals("3", res.getValue(row3Idx, "user_id"));
        assertEquals("1", res.getValue(row3Idx, "count(*)"));
        assertEquals("400.0", res.getValue(row3Idx, "sum(amount)"));

        txn.commit();
    }

    @Test
    void testSqlTransactionalRollback() {
        // Create table first
        Transaction txnInit = transactionManager.begin(
                Transaction.IsolationLevel.SNAPSHOT_ISOLATION,
                Transaction.ConcurrencyMode.OPTIMISTIC
        );
        sqlExecutor.execute(txnInit, "CREATE TABLE users (id INT, name VARCHAR)");
        txnInit.commit();

        // Perform insert inside transaction that aborts
        Transaction txnWrite = transactionManager.begin(
                Transaction.IsolationLevel.SNAPSHOT_ISOLATION,
                Transaction.ConcurrencyMode.OPTIMISTIC
        );
        sqlExecutor.execute(txnWrite, "INSERT INTO users (id, name) VALUES (1, 'Alice')");
        // Abort!
        txnWrite.abort();

        // Start new transaction, table should be empty
        Transaction txnRead = transactionManager.begin(
                Transaction.IsolationLevel.SNAPSHOT_ISOLATION,
                Transaction.ConcurrencyMode.OPTIMISTIC
        );
        ResultSet res = sqlExecutor.execute(txnRead, "SELECT * FROM users");
        assertEquals(0, res.getRowCount());
        txnRead.commit();
    }
}
