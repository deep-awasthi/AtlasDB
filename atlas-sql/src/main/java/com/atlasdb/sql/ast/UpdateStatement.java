package com.atlasdb.sql.ast;

import java.util.Map;

/**
 * AST node for DML "UPDATE".
 */
public final class UpdateStatement implements Statement {
    private final String tableName;
    private final Map<String, String> setClauses;
    private final WhereClause whereClause;

    public UpdateStatement(String tableName, Map<String, String> setClauses, WhereClause whereClause) {
        this.tableName = tableName;
        this.setClauses = setClauses;
        this.whereClause = whereClause;
    }

    public String getTableName() {
        return tableName;
    }

    public Map<String, String> getSetClauses() {
        return setClauses;
    }

    public WhereClause getWhereClause() {
        return whereClause;
    }

    @Override
    public String toString() {
        return "UpdateStatement{" +
                "tableName='" + tableName + '\'' +
                ", setClauses=" + setClauses +
                ", whereClause=" + whereClause +
                '}';
    }
}
