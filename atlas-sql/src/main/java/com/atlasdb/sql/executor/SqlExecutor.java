package com.atlasdb.sql.executor;

import com.atlasdb.sql.ast.*;
import com.atlasdb.sql.catalog.CatalogManager;
import com.atlasdb.sql.parser.Lexer;
import com.atlasdb.sql.parser.SqlParser;
import com.atlasdb.sql.parser.Token;
import com.atlasdb.storage.index.KeyValueParser;
import com.atlasdb.transaction.Transaction;

import java.util.*;

/**
 * Executes parsed SQL Statements inside the context of an active Transaction.
 * Provides DDL (CREATE TABLE), DML (INSERT, UPDATE, DELETE), and DQL (SELECT
 * with filters, nested loop joins, groupings, and aggregations).
 */
public final class SqlExecutor {

    private final CatalogManager catalogManager;

    public SqlExecutor(CatalogManager catalogManager) {
        this.catalogManager = catalogManager;
    }

    /**
     * Executes an SQL query string transactionally.
     *
     * @param txn the active transaction
     * @param sql the SQL command string
     * @return the ResultSet of execution
     */
    public ResultSet execute(Transaction txn, String sql) {
        if (sql == null || sql.trim().isEmpty()) {
            throw new IllegalArgumentException("SQL statement cannot be null or empty.");
        }

        // Standardize sql statement (remove trailing semicolon)
        String trimmedSql = sql.trim();
        if (trimmedSql.endsWith(";")) {
            trimmedSql = trimmedSql.substring(0, trimmedSql.length() - 1);
        }

        Lexer lexer = new Lexer(trimmedSql);
        List<Token> tokens = lexer.tokenize();
        SqlParser parser = new SqlParser(tokens);
        Statement stmt = parser.parse();

        if (stmt instanceof CreateTableStatement) {
            return executeCreate(txn, (CreateTableStatement) stmt);
        } else if (stmt instanceof InsertStatement) {
            return executeInsert(txn, (InsertStatement) stmt);
        } else if (stmt instanceof SelectStatement) {
            return executeSelect(txn, (SelectStatement) stmt);
        } else if (stmt instanceof UpdateStatement) {
            return executeUpdate(txn, (UpdateStatement) stmt);
        } else if (stmt instanceof DeleteStatement) {
            return executeDelete(txn, (DeleteStatement) stmt);
        }

        throw new UnsupportedOperationException("Unsupported statement type: " + stmt.getClass().getSimpleName());
    }

    private ResultSet executeCreate(Transaction txn, CreateTableStatement stmt) {
        catalogManager.createTable(txn, stmt.getTableName(), stmt.getColumns());
        return new ResultSet(
                List.of("Status"),
                List.of(List.of("Table '" + stmt.getTableName() + "' created successfully."))
        );
    }

    private ResultSet executeInsert(Transaction txn, InsertStatement stmt) {
        CatalogManager.TableSchema schema = catalogManager.getTableSchema(txn, stmt.getTableName());
        if (schema == null) {
            throw new IllegalArgumentException("Table '" + stmt.getTableName() + "' does not exist.");
        }

        List<ColumnDef> columns = schema.getColumns();
        List<String> valuesToInsert = new ArrayList<>(Collections.nCopies(columns.size(), ""));

        // Match column values
        if (stmt.getColumns().isEmpty()) {
            // Columns not specified, assume standard schema order
            if (stmt.getValues().size() != columns.size()) {
                throw new IllegalArgumentException("Column count mismatch. Expected " + columns.size() + " values.");
            }
            for (int i = 0; i < columns.size(); i++) {
                valuesToInsert.set(i, stmt.getValues().get(i));
            }
        } else {
            // Match specified columns
            if (stmt.getColumns().size() != stmt.getValues().size()) {
                throw new IllegalArgumentException("Values list count must match columns list count.");
            }
            for (int i = 0; i < stmt.getColumns().size(); i++) {
                String colName = stmt.getColumns().get(i);
                String val = stmt.getValues().get(i);
                int idx = schema.getColumnIndex(colName);
                if (idx == -1) {
                    throw new IllegalArgumentException("Column '" + colName + "' not found in table schema.");
                }
                valuesToInsert.set(idx, val);
            }
        }

        // Validate types and constraints
        String primaryKeyVal = null;
        for (int i = 0; i < columns.size(); i++) {
            ColumnDef col = columns.get(i);
            String val = valuesToInsert.get(i);

            // Primary Key cannot be null
            if (col.name().equalsIgnoreCase(schema.getPrimaryKey())) {
                if (val == null || val.isEmpty()) {
                    throw new IllegalArgumentException("Primary Key '" + col.name() + "' cannot be null/empty.");
                }
                primaryKeyVal = val;
            }

            // Simple type check for INT
            if ("INT".equalsIgnoreCase(col.type()) && !val.isEmpty()) {
                try {
                    Integer.parseInt(val);
                } catch (NumberFormatException nfe) {
                    throw new IllegalArgumentException("Invalid type for column '" + col.name() + "'. Expected INT.");
                }
            }
        }

        // Format record string: col1=val1,col2=val2...
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < columns.size(); i++) {
            sb.append(columns.get(i).name()).append("=").append(valuesToInsert.get(i));
            if (i < columns.size() - 1) {
                sb.append(",");
            }
        }

        String dbKey = "table:" + stmt.getTableName().toLowerCase() + ":" + primaryKeyVal;
        txn.put(dbKey, sb.toString());

        return new ResultSet(
                List.of("Status"),
                List.of(List.of("1 row inserted."))
        );
    }

    private ResultSet executeSelect(Transaction txn, SelectStatement stmt) {
        CatalogManager.TableSchema schema = catalogManager.getTableSchema(txn, stmt.getTableName());
        if (schema == null) {
            throw new IllegalArgumentException("Table '" + stmt.getTableName() + "' does not exist.");
        }

        // Scan main table rows
        List<String> rawRows = scanTableRaw(txn, schema);
        List<Map<String, String>> rowMaps = new ArrayList<>();
        for (String raw : rawRows) {
            rowMaps.add(parseRowToMap(raw, schema));
        }

        // Process JOIN
        if (stmt.getJoinClause() != null) {
            JoinClause join = stmt.getJoinClause();
            CatalogManager.TableSchema joinSchema = catalogManager.getTableSchema(txn, join.getJoinTable());
            if (joinSchema == null) {
                throw new IllegalArgumentException("Join table '" + join.getJoinTable() + "' does not exist.");
            }

            List<String> rawJoinRows = scanTableRaw(txn, joinSchema);
            List<Map<String, String>> joinRowMaps = new ArrayList<>();
            for (String raw : rawJoinRows) {
                joinRowMaps.add(parseRowToMap(raw, joinSchema));
            }

            // Implement Nested Loop Join
            List<Map<String, String>> joinedMaps = new ArrayList<>();
            for (Map<String, String> r1 : rowMaps) {
                for (Map<String, String> r2 : joinRowMaps) {
                    String leftVal = getColumnValueQualified(r1, join.getLeftColumn());
                    String rightVal = getColumnValueQualified(r2, join.getRightColumn());

                    if (leftVal != null && leftVal.equals(rightVal)) {
                        Map<String, String> joined = new LinkedHashMap<>();
                        // Add qualified names to resolve conflicts: tableName.colName
                        for (Map.Entry<String, String> e : r1.entrySet()) {
                            joined.put(schema.getTableName().toLowerCase() + "." + e.getKey(), e.getValue());
                        }
                        for (Map.Entry<String, String> e : r2.entrySet()) {
                            joined.put(joinSchema.getTableName().toLowerCase() + "." + e.getKey(), e.getValue());
                        }
                        joinedMaps.add(joined);
                    }
                }
            }
            rowMaps = joinedMaps;
        }

        // Process WHERE filter
        if (stmt.getWhereClause() != null) {
            WhereClause where = stmt.getWhereClause();
            List<Map<String, String>> filtered = new ArrayList<>();
            for (Map<String, String> row : rowMaps) {
                String val = getColumnValueQualified(row, where.getColumn());
                if (evaluateComparison(val, where.getOperator(), where.getValue())) {
                    filtered.add(row);
                }
            }
            rowMaps = filtered;
        }

        // Process GROUP BY & Aggregations
        boolean hasAggregates = false;
        for (String field : stmt.getSelectFields()) {
            if (isAggregateField(field)) {
                hasAggregates = true;
                break;
            }
        }

        if (hasAggregates || stmt.getGroupByField() != null) {
            rowMaps = executeAggregation(rowMaps, stmt, schema);
        }

        // Build ResultSet Header (columns list)
        List<String> outputHeader = new ArrayList<>();
        if (stmt.getSelectFields().size() == 1 && stmt.getSelectFields().get(0).equals("*")) {
            // Select all: project all keys present in first row
            if (!rowMaps.isEmpty()) {
                outputHeader.addAll(rowMaps.get(0).keySet());
            } else {
                for (ColumnDef col : schema.getColumns()) {
                    outputHeader.add(col.name());
                }
            }
        } else {
            outputHeader.addAll(stmt.getSelectFields());
        }

        // Build Projection rows
        List<List<String>> outputRows = new ArrayList<>();
        for (Map<String, String> row : rowMaps) {
            List<String> projected = new ArrayList<>();
            for (String field : outputHeader) {
                String cell = getColumnValueQualified(row, field);
                projected.add(cell != null ? cell : "NULL");
            }
            outputRows.add(projected);
        }

        // Process LIMIT
        if (stmt.getLimit() != null && outputRows.size() > stmt.getLimit()) {
            outputRows = outputRows.subList(0, stmt.getLimit());
        }

        return new ResultSet(outputHeader, outputRows);
    }

    private ResultSet executeUpdate(Transaction txn, UpdateStatement stmt) {
        CatalogManager.TableSchema schema = catalogManager.getTableSchema(txn, stmt.getTableName());
        if (schema == null) {
            throw new IllegalArgumentException("Table '" + stmt.getTableName() + "' does not exist.");
        }

        List<String> rawRows = scanTableRaw(txn, schema);
        int updateCount = 0;

        for (String raw : rawRows) {
            Map<String, String> row = parseRowToMap(raw, schema);
            
            // Check WHERE filter
            boolean matches = true;
            if (stmt.getWhereClause() != null) {
                WhereClause where = stmt.getWhereClause();
                String val = row.get(where.getColumn());
                matches = evaluateComparison(val, where.getOperator(), where.getValue());
            }

            if (matches) {
                // Apply update
                for (Map.Entry<String, String> set : stmt.getSetClauses().entrySet()) {
                    int idx = schema.getColumnIndex(set.getKey());
                    if (idx == -1) {
                        throw new IllegalArgumentException("Column '" + set.getKey() + "' not found.");
                    }
                    row.put(set.getKey().toLowerCase(), set.getValue());
                }

                // Format record back to string
                StringBuilder sb = new StringBuilder();
                List<ColumnDef> columns = schema.getColumns();
                for (int i = 0; i < columns.size(); i++) {
                    String colName = columns.get(i).name();
                    sb.append(colName).append("=").append(row.get(colName.toLowerCase()));
                    if (i < columns.size() - 1) {
                        sb.append(",");
                    }
                }

                String pkVal = row.get(schema.getPrimaryKey().toLowerCase());
                String dbKey = "table:" + stmt.getTableName().toLowerCase() + ":" + pkVal;
                txn.put(dbKey, sb.toString());
                updateCount++;
            }
        }

        return new ResultSet(
                List.of("Status"),
                List.of(List.of(updateCount + " rows updated."))
        );
    }

    private ResultSet executeDelete(Transaction txn, DeleteStatement stmt) {
        CatalogManager.TableSchema schema = catalogManager.getTableSchema(txn, stmt.getTableName());
        if (schema == null) {
            throw new IllegalArgumentException("Table '" + stmt.getTableName() + "' does not exist.");
        }

        List<String> rawRows = scanTableRaw(txn, schema);
        int deleteCount = 0;

        for (String raw : rawRows) {
            Map<String, String> row = parseRowToMap(raw, schema);

            // Check WHERE filter
            boolean matches = true;
            if (stmt.getWhereClause() != null) {
                WhereClause where = stmt.getWhereClause();
                String val = row.get(where.getColumn());
                matches = evaluateComparison(val, where.getOperator(), where.getValue());
            }

            if (matches) {
                String pkVal = row.get(schema.getPrimaryKey().toLowerCase());
                String dbKey = "table:" + stmt.getTableName().toLowerCase() + ":" + pkVal;
                txn.delete(dbKey);
                deleteCount++;
            }
        }

        return new ResultSet(
                List.of("Status"),
                List.of(List.of(deleteCount + " rows deleted."))
        );
    }

    private List<String> scanTableRaw(Transaction txn, CatalogManager.TableSchema schema) {
        List<String> rawRows = new ArrayList<>();
        String prefix = "table:" + schema.getTableName().toLowerCase() + ":";
        Set<String> processedKeys = new HashSet<>();

        // 1. Scan storage engine committed entries
        Iterator<?> it = txn.getStorageEngine().iterator();
        while (it.hasNext()) {
            com.atlasdb.storage.Entry<?, ?> entry = (com.atlasdb.storage.Entry<?, ?>) it.next();
            String key = (String) entry.getKey();
            if (key.startsWith(prefix)) {
                processedKeys.add(key);
                String valStr = txn.get(key); // Snapshot-isolated read
                if (valStr != null) {
                    rawRows.add(valStr);
                }
            }
        }

        // 2. Scan transaction private write buffer for uncommitted writes/inserts
        for (String key : txn.getWriteBuffer().keySet()) {
            if (key.startsWith(prefix) && !processedKeys.contains(key)) {
                String valStr = txn.get(key);
                if (valStr != null) {
                    rawRows.add(valStr);
                }
            }
        }

        return rawRows;
    }

    private Map<String, String> parseRowToMap(String rawRecord, CatalogManager.TableSchema schema) {
        Map<String, String> map = new LinkedHashMap<>();
        for (ColumnDef col : schema.getColumns()) {
            String val = KeyValueParser.parseFieldValue(rawRecord, col.name());
            map.put(col.name().toLowerCase(), val != null ? val : "");
        }
        return map;
    }

    private String getColumnValueQualified(Map<String, String> row, String field) {
        String target = field.toLowerCase().trim();
        if (row.containsKey(target)) {
            return row.get(target);
        }
        // If field is qualified (e.g. users.id) but row contains simple key (id)
        if (target.contains(".")) {
            String colPart = target.substring(target.lastIndexOf('.') + 1);
            if (row.containsKey(colPart)) {
                return row.get(colPart);
            }
        }
        // If field is simple (id) but row contains qualified key (users.id)
        for (Map.Entry<String, String> e : row.entrySet()) {
            if (e.getKey().endsWith("." + target)) {
                return e.getValue();
            }
        }
        return null;
    }

    private boolean evaluateComparison(String val, String op, String expected) {
        if (val == null) {
            return false;
        }

        // Simple string comparison for equals
        if ("=".equals(op)) {
            return val.equalsIgnoreCase(expected);
        }

        // Numeric comparison for >, <
        try {
            double vNum = Double.parseDouble(val);
            double expNum = Double.parseDouble(expected);
            if (">".equals(op)) {
                return vNum > expNum;
            }
            if ("<".equals(op)) {
                return vNum < expNum;
            }
        } catch (NumberFormatException nfe) {
            // Fallback to lexicographical comparison for strings
            int cmp = val.compareToIgnoreCase(expected);
            if (">".equals(op)) {
                return cmp > 0;
            }
            if ("<".equals(op)) {
                return cmp < 0;
            }
        }
        return false;
    }

    private boolean isAggregateField(String field) {
        String f = field.toLowerCase().trim();
        return f.startsWith("count(") || f.startsWith("sum(") || f.startsWith("avg(");
    }

    private List<Map<String, String>> executeAggregation(List<Map<String, String>> rows, SelectStatement stmt, CatalogManager.TableSchema schema) {
        String groupCol = stmt.getGroupByField() != null ? stmt.getGroupByField().toLowerCase() : null;

        // Group rows: groupVal -> List of rows
        Map<String, List<Map<String, String>>> groups = new LinkedHashMap<>();
        if (groupCol != null) {
            for (Map<String, String> row : rows) {
                String val = getColumnValueQualified(row, groupCol);
                String key = val != null ? val : "NULL";
                groups.computeIfAbsent(key, k -> new ArrayList<>()).add(row);
            }
        } else {
            groups.put("ALL", rows);
        }

        List<Map<String, String>> result = new ArrayList<>();
        for (Map.Entry<String, List<Map<String, String>>> group : groups.entrySet()) {
            Map<String, String> aggRow = new LinkedHashMap<>();
            if (groupCol != null) {
                aggRow.put(groupCol, group.getKey());
            }

            for (String field : stmt.getSelectFields()) {
                if (isAggregateField(field)) {
                    String func = field.substring(0, field.indexOf('(')).toLowerCase().trim();
                    String arg = field.substring(field.indexOf('(') + 1, field.indexOf(')')).trim();

                    if ("count".equalsIgnoreCase(func)) {
                        long count = 0;
                        if ("*".equals(arg)) {
                            count = group.getValue().size();
                        } else {
                            for (Map<String, String> row : group.getValue()) {
                                String val = getColumnValueQualified(row, arg);
                                if (val != null && !val.isEmpty()) {
                                    count++;
                                }
                            }
                        }
                        aggRow.put(field, String.valueOf(count));
                    } else if ("sum".equalsIgnoreCase(func)) {
                        double sum = 0;
                        for (Map<String, String> row : group.getValue()) {
                            String val = getColumnValueQualified(row, arg);
                            if (val != null && !val.isEmpty()) {
                                try {
                                    sum += Double.parseDouble(val);
                                } catch (NumberFormatException ignored) {}
                            }
                        }
                        aggRow.put(field, String.valueOf(sum));
                    }
                } else if (groupCol != null && field.equalsIgnoreCase(groupCol)) {
                    // Already added Group By column
                }
            }
            result.add(aggRow);
        }

        return result;
    }
}
