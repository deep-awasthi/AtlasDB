package com.atlasdb.sql.ast;

/**
 * AST node for DML "DELETE FROM".
 */
public final class DeleteStatement implements Statement {
    private final String tableName;
    private final WhereClause whereClause;

    public DeleteStatement(String tableName, WhereClause whereClause) {
        this.tableName = tableName;
        this.whereClause = whereClause;
    }

    public String getTableName() {
        return tableName;
    }

    public WhereClause getWhereClause() {
        return whereClause;
    }

    @Override
    public String toString() {
        return "DeleteStatement{" +
                "tableName='" + tableName + '\'' +
                ", whereClause=" + whereClause +
                '}';
    }
}
