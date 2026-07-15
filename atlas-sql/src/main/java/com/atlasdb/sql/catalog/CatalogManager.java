package com.atlasdb.sql.catalog;

import com.atlasdb.sql.ast.ColumnDef;
import com.atlasdb.transaction.Transaction;

import java.util.*;

/**
 * Manages database schemas and tables metadata, stored transactionally
 * using the storage engine catalog namespace.
 */
public final class CatalogManager {

    private static final String CATALOG_PREFIX = "catalog:";

    public static final class TableSchema {
        private final String tableName;
        private final List<ColumnDef> columns;
        private final String primaryKey;

        public TableSchema(String tableName, List<ColumnDef> columns, String primaryKey) {
            this.tableName = tableName;
            this.columns = columns;
            this.primaryKey = primaryKey;
        }

        public String getTableName() {
            return tableName;
        }

        public List<ColumnDef> getColumns() {
            return columns;
        }

        public String getPrimaryKey() {
            return primaryKey;
        }

        /**
         * Resolves the index position of a column in the schema.
         *
         * @param colName the column name
         * @return index position, or -1 if not found
         */
        public int getColumnIndex(String colName) {
            // Support fully-qualified names like "users.name" by matching simple suffix name
            String target = colName.contains(".") ? colName.substring(colName.lastIndexOf('.') + 1) : colName;
            for (int i = 0; i < columns.size(); i++) {
                if (columns.get(i).name().equalsIgnoreCase(target)) {
                    return i;
                }
            }
            return -1;
        }
    }

    /**
     * Creates a new table schema definition in the database catalog.
     *
     * @param txn       the active transaction
     * @param tableName the table name
     * @param columns   the column definitions
     */
    public void createTable(Transaction txn, String tableName, List<ColumnDef> columns) {
        if (columns == null || columns.isEmpty()) {
            throw new IllegalArgumentException("Table must define at least one column.");
        }

        // Schema format: colName1:type1,colName2:type2...
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < columns.size(); i++) {
            ColumnDef col = columns.get(i);
            sb.append(col.name()).append(":").append(col.type());
            if (i < columns.size() - 1) {
                sb.append(",");
            }
        }

        String key = CATALOG_PREFIX + tableName.toLowerCase();
        if (txn.get(key) != null) {
            throw new IllegalStateException("Table '" + tableName + "' already exists.");
        }

        txn.put(key, sb.toString());
    }

    /**
     * Resolves the schema definition of a table in the database catalog.
     *
     * @param txn       the active transaction
     * @param tableName the table name
     * @return the table schema, or null if table does not exist
     */
    public TableSchema getTableSchema(Transaction txn, String tableName) {
        String key = CATALOG_PREFIX + tableName.toLowerCase();
        String schemaStr = txn.get(key);
        if (schemaStr == null) {
            return null;
        }

        List<ColumnDef> columns = new ArrayList<>();
        String[] parts = schemaStr.split(",");
        for (String p : parts) {
            String[] colParts = p.split(":");
            columns.add(new ColumnDef(colParts[0], colParts[1]));
        }

        // Treat the first column defined as the primary key
        String primaryKey = columns.isEmpty() ? null : columns.get(0).name();

        return new TableSchema(tableName, columns, primaryKey);
    }
}
