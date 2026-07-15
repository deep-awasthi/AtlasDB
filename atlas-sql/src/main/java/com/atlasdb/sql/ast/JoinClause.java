package com.atlasdb.sql.ast;

/**
 * Represents a JOIN clause in a SELECT query (e.g. JOIN orders ON users.id = orders.user_id).
 */
public final class JoinClause {
    private final String joinTable;
    private final String leftColumn;
    private final String rightColumn;

    public JoinClause(String joinTable, String leftColumn, String rightColumn) {
        this.joinTable = joinTable;
        this.leftColumn = leftColumn;
        this.rightColumn = rightColumn;
    }

    public String getJoinTable() {
        return joinTable;
    }

    public String getLeftColumn() {
        return leftColumn;
    }

    public String getRightColumn() {
        return rightColumn;
    }

    @Override
    public String toString() {
        return "JoinClause{" +
                "joinTable='" + joinTable + '\'' +
                ", leftColumn='" + leftColumn + '\'' +
                ", rightColumn='" + rightColumn + '\'' +
                '}';
    }
}
