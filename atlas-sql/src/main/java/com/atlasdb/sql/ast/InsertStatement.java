package com.atlasdb.sql.ast;

import java.util.List;

/**
 * AST node for DML "INSERT INTO".
 */
public final class InsertStatement implements Statement {
    private final String tableName;
    private final List<String> columns;
    private final List<String> values;

    public InsertStatement(String tableName, List<String> columns, List<String> values) {
        this.tableName = tableName;
        this.columns = columns;
        this.values = values;
    }

    public String getTableName() {
        return tableName;
    }

    public List<String> getColumns() {
        return columns;
    }

    public List<String> getValues() {
        return values;
    }

    @Override
    public String toString() {
        return "InsertStatement{" +
                "tableName='" + tableName + '\'' +
                ", columns=" + columns +
                ", values=" + values +
                '}';
    }
}
