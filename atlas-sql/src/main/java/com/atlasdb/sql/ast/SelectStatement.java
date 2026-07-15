package com.atlasdb.sql.ast;

import java.util.List;

/**
 * AST node for DQL "SELECT".
 */
public final class SelectStatement implements Statement {
    private final List<String> selectFields;
    private final String tableName;
    private final JoinClause joinClause;
    private final WhereClause whereClause;
    private final String groupByField;
    private final Integer limit;

    public SelectStatement(List<String> selectFields, String tableName, JoinClause joinClause,
                           WhereClause whereClause, String groupByField, Integer limit) {
        this.selectFields = selectFields;
        this.tableName = tableName;
        this.joinClause = joinClause;
        this.whereClause = whereClause;
        this.groupByField = groupByField;
        this.limit = limit;
    }

    public List<String> getSelectFields() {
        return selectFields;
    }

    public String getTableName() {
        return tableName;
    }

    public JoinClause getJoinClause() {
        return joinClause;
    }

    public WhereClause getWhereClause() {
        return whereClause;
    }

    public String getGroupByField() {
        return groupByField;
    }

    public Integer getLimit() {
        return limit;
    }

    @Override
    public String toString() {
        return "SelectStatement{" +
                "selectFields=" + selectFields +
                ", tableName='" + tableName + '\'' +
                ", joinClause=" + joinClause +
                ", whereClause=" + whereClause +
                ", groupByField='" + groupByField + '\'' +
                ", limit=" + limit +
                '}';
    }
}
