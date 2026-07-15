package com.atlasdb.sql.ast;

/**
 * Represents a column definition in a CREATE TABLE statement.
 *
 * @param name the column name
 * @param type the column type (INT or VARCHAR)
 */
public record ColumnDef(String name, String type) {
}
