package com.atlasdb.sql.parser;

/**
 * Enumeration of token types in our SQL syntax.
 */
public enum TokenType {
    // Keywords
    CREATE, TABLE, INSERT, INTO, VALUES, SELECT, FROM, JOIN, ON, WHERE, UPDATE, SET, DELETE, GROUP, BY, LIMIT, AND, OR,
    
    // Data Types
    INT, VARCHAR,

    // Identifiers and Literals
    IDENTIFIER, NUMBER, STRING,

    // Symbols
    LPAREN, // (
    RPAREN, // )
    COMMA,  // ,
    EQUALS, // =
    STAR,   // *
    GT,     // >
    LT,     // <

    // Special
    EOF
}
