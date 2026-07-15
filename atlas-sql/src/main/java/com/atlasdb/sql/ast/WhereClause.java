package com.atlasdb.sql.ast;

/**
 * Represents a WHERE filtering clause (e.g. col = val, col > val).
 */
public final class WhereClause {
    private final String column;
    private final String operator;
    private final String value;

    public WhereClause(String column, String operator, String value) {
        this.column = column;
        this.operator = operator;
        this.value = value;
    }

    public String getColumn() {
        return column;
    }

    public String getOperator() {
        return operator;
    }

    public String getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "WhereClause{" +
                "column='" + column + '\'' +
                ", operator='" + operator + '\'' +
                ", value='" + value + '\'' +
                '}';
    }
}
