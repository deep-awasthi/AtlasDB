package com.atlasdb.sql.executor;

import java.util.*;

/**
 * Tabular result set representing rows and columns returned by SQL queries.
 */
public final class ResultSet {
    private final List<String> columnNames;
    private final List<List<String>> rows;

    public ResultSet(List<String> columnNames, List<List<String>> rows) {
        this.columnNames = columnNames;
        this.rows = rows;
    }

    public List<String> getColumnNames() {
        return columnNames;
    }

    public List<List<String>> getRows() {
        return rows;
    }

    public int getColumnCount() {
        return columnNames.size();
    }

    public int getRowCount() {
        return rows.size();
    }

    /**
     * Resolves the cell value by row index and column name.
     *
     * @param rowIndex the row index (0-based)
     * @param colName  the target column name
     * @return the cell string value, or null if column not found
     */
    public String getValue(int rowIndex, String colName) {
        int colIdx = -1;
        for (int i = 0; i < columnNames.size(); i++) {
            // Support simple matching or qualified table.column matching
            String col = columnNames.get(i);
            if (col.equalsIgnoreCase(colName) || 
                (colName.contains(".") && col.endsWith(colName.substring(colName.indexOf('.'))))) {
                colIdx = i;
                break;
            }
        }
        if (colIdx == -1) {
            return null;
        }
        return rows.get(rowIndex).get(colIdx);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        // Header
        sb.append(String.join("\t| ", columnNames)).append("\n");
        sb.append("-".repeat(Math.max(10, columnNames.size() * 15))).append("\n");
        // Rows
        for (List<String> row : rows) {
            sb.append(String.join("\t| ", row)).append("\n");
        }
        return sb.toString();
    }
}
