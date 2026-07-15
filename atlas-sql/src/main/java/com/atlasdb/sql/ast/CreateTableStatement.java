package com.atlasdb.sql.ast;

import java.util.List;

/**
 * AST node for DDL "CREATE TABLE".
 */
public final class CreateTableStatement implements Statement {
    private final String tableName;
    private final List<ColumnDef> columns;

    public CreateTableStatement(String tableName, List<ColumnDef> columns) {
        this.tableName = tableName;
        this.columns = columns;
    }

    public String getTableName() {
        return tableName;
    }

    public List<ColumnDef> getColumns() {
        return columns;
    }

    @Override
    public String toString() {
        return "CreateTableStatement{" +
                "tableName='" + tableName + '\'' +
                ", columns=" + columns +
                '}';
    }
}
