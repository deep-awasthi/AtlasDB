package com.atlasdb.sql.parser;

import com.atlasdb.sql.ast.*;

import java.util.*;

/**
 * Hand-written recursive descent parser compiling SQL token lists into AST Statement nodes.
 */
public final class SqlParser {

    private final List<Token> tokens;
    private int cur = 0;

    public SqlParser(List<Token> tokens) {
        this.tokens = tokens;
    }

    private Token peek() {
        if (cur >= tokens.size()) {
            return new Token(TokenType.EOF, "");
        }
        return tokens.get(cur);
    }

    private Token next() {
        Token t = peek();
        if (t.getType() != TokenType.EOF) {
            cur++;
        }
        return t;
    }

    private boolean match(TokenType type) {
        if (peek().getType() == type) {
            next();
            return true;
        }
        return false;
    }

    private Token consume(TokenType expectedType, String errorMsg) {
        Token t = peek();
        if (t.getType() != expectedType) {
            throw new IllegalArgumentException(errorMsg + " (Found: " + t + " at index " + cur + ")");
        }
        return next();
    }

    /**
     * Parses the SQL tokens into a Statement AST node.
     *
     * @return the parsed Statement node
     */
    public Statement parse() {
        TokenType first = peek().getType();
        switch (first) {
            case CREATE:
                return parseCreateTable();
            case INSERT:
                return parseInsert();
            case SELECT:
                return parseSelect();
            case UPDATE:
                return parseUpdate();
            case DELETE:
                return parseDelete();
            default:
                throw new IllegalArgumentException("Unsupported SQL statement beginning with " + peek().getValue());
        }
    }

    private CreateTableStatement parseCreateTable() {
        consume(TokenType.CREATE, "Expected CREATE");
        consume(TokenType.TABLE, "Expected TABLE");
        String tableName = consume(TokenType.IDENTIFIER, "Expected table name identifier").getValue();
        consume(TokenType.LPAREN, "Expected open parenthesis '('");

        List<ColumnDef> columns = new ArrayList<>();
        while (peek().getType() != TokenType.RPAREN && peek().getType() != TokenType.EOF) {
            String colName = consume(TokenType.IDENTIFIER, "Expected column name").getValue();
            
            // Parse Column Type
            Token typeTok = next();
            if (typeTok.getType() != TokenType.INT && typeTok.getType() != TokenType.VARCHAR) {
                throw new IllegalArgumentException("Expected column type INT or VARCHAR, found " + typeTok.getValue());
            }
            String colType = typeTok.getValue();
            
            columns.add(new ColumnDef(colName, colType));

            if (peek().getType() == TokenType.COMMA) {
                consume(TokenType.COMMA, "Expected comma");
            }
        }
        consume(TokenType.RPAREN, "Expected closing parenthesis ')'");
        return new CreateTableStatement(tableName, columns);
    }

    private InsertStatement parseInsert() {
        consume(TokenType.INSERT, "Expected INSERT");
        consume(TokenType.INTO, "Expected INTO");
        String tableName = consume(TokenType.IDENTIFIER, "Expected table name identifier").getValue();

        List<String> columns = new ArrayList<>();
        if (match(TokenType.LPAREN)) {
            while (peek().getType() != TokenType.RPAREN && peek().getType() != TokenType.EOF) {
                columns.add(consume(TokenType.IDENTIFIER, "Expected column identifier").getValue());
                if (peek().getType() == TokenType.COMMA) {
                    consume(TokenType.COMMA, "Expected comma");
                }
            }
            consume(TokenType.RPAREN, "Expected closing parenthesis ')'");
        }

        consume(TokenType.VALUES, "Expected VALUES");
        consume(TokenType.LPAREN, "Expected open parenthesis '('");

        List<String> values = new ArrayList<>();
        while (peek().getType() != TokenType.RPAREN && peek().getType() != TokenType.EOF) {
            Token valTok = next();
            if (valTok.getType() != TokenType.STRING && valTok.getType() != TokenType.NUMBER && valTok.getType() != TokenType.IDENTIFIER) {
                throw new IllegalArgumentException("Expected string, number, or identifier literal value, found " + valTok);
            }
            values.add(valTok.getValue());

            if (peek().getType() == TokenType.COMMA) {
                consume(TokenType.COMMA, "Expected comma");
            }
        }
        consume(TokenType.RPAREN, "Expected closing parenthesis ')'");

        return new InsertStatement(tableName, columns, values);
    }

    private SelectStatement parseSelect() {
        consume(TokenType.SELECT, "Expected SELECT");

        List<String> selectFields = new ArrayList<>();
        if (match(TokenType.STAR)) {
            selectFields.add("*");
        } else {
            while (true) {
                String fieldName = consume(TokenType.IDENTIFIER, "Expected column identifier").getValue();
                if (peek().getType() == TokenType.LPAREN) {
                    consume(TokenType.LPAREN, "Expected '('");
                    String arg;
                    if (peek().getType() == TokenType.STAR) {
                        arg = consume(TokenType.STAR, "Expected '*'").getValue();
                    } else {
                        arg = consume(TokenType.IDENTIFIER, "Expected column identifier").getValue();
                    }
                    consume(TokenType.RPAREN, "Expected ')'");
                    fieldName = fieldName + "(" + arg + ")";
                }
                selectFields.add(fieldName);
                if (peek().getType() == TokenType.COMMA) {
                    consume(TokenType.COMMA, "Expected comma");
                } else {
                    break;
                }
            }
        }

        consume(TokenType.FROM, "Expected FROM");
        String tableName = consume(TokenType.IDENTIFIER, "Expected table name").getValue();

        // Optional JOIN
        JoinClause joinClause = null;
        if (peek().getType() == TokenType.JOIN) {
            consume(TokenType.JOIN, "Expected JOIN");
            String joinTable = consume(TokenType.IDENTIFIER, "Expected join table name").getValue();
            consume(TokenType.ON, "Expected ON");
            String leftCol = consume(TokenType.IDENTIFIER, "Expected join left column").getValue();
            consume(TokenType.EQUALS, "Expected '='");
            String rightCol = consume(TokenType.IDENTIFIER, "Expected join right column").getValue();
            joinClause = new JoinClause(joinTable, leftCol, rightCol);
        }

        // Optional WHERE
        WhereClause whereClause = null;
        if (peek().getType() == TokenType.WHERE) {
            consume(TokenType.WHERE, "Expected WHERE");
            String col = consume(TokenType.IDENTIFIER, "Expected column name").getValue();
            
            Token opTok = next();
            if (opTok.getType() != TokenType.EQUALS && opTok.getType() != TokenType.GT && opTok.getType() != TokenType.LT) {
                throw new IllegalArgumentException("Expected comparison operator (=, >, <), found " + opTok);
            }
            String op = opTok.getValue();

            Token valTok = next();
            if (valTok.getType() != TokenType.STRING && valTok.getType() != TokenType.NUMBER && valTok.getType() != TokenType.IDENTIFIER) {
                throw new IllegalArgumentException("Expected literal value, found " + valTok);
            }
            String val = valTok.getValue();
            whereClause = new WhereClause(col, op, val);
        }

        // Optional GROUP BY
        String groupByField = null;
        if (peek().getType() == TokenType.GROUP) {
            consume(TokenType.GROUP, "Expected GROUP");
            consume(TokenType.BY, "Expected BY");
            groupByField = consume(TokenType.IDENTIFIER, "Expected group by column").getValue();
        }

        // Optional LIMIT
        Integer limit = null;
        if (peek().getType() == TokenType.LIMIT) {
            consume(TokenType.LIMIT, "Expected LIMIT");
            limit = Integer.parseInt(consume(TokenType.NUMBER, "Expected limit numeric value").getValue());
        }

        return new SelectStatement(selectFields, tableName, joinClause, whereClause, groupByField, limit);
    }

    private UpdateStatement parseUpdate() {
        consume(TokenType.UPDATE, "Expected UPDATE");
        String tableName = consume(TokenType.IDENTIFIER, "Expected table name").getValue();
        consume(TokenType.SET, "Expected SET");

        Map<String, String> setClauses = new LinkedHashMap<>();
        while (true) {
            String colName = consume(TokenType.IDENTIFIER, "Expected column name").getValue();
            consume(TokenType.EQUALS, "Expected '='");
            Token valTok = next();
            if (valTok.getType() != TokenType.STRING && valTok.getType() != TokenType.NUMBER) {
                throw new IllegalArgumentException("Expected string or number literal, found " + valTok);
            }
            setClauses.put(colName, valTok.getValue());

            if (peek().getType() == TokenType.COMMA) {
                consume(TokenType.COMMA, "Expected comma");
            } else {
                break;
            }
        }

        // Optional WHERE
        WhereClause whereClause = null;
        if (peek().getType() == TokenType.WHERE) {
            consume(TokenType.WHERE, "Expected WHERE");
            String col = consume(TokenType.IDENTIFIER, "Expected column name").getValue();
            consume(TokenType.EQUALS, "Expected '='");
            Token valTok = next();
            if (valTok.getType() != TokenType.STRING && valTok.getType() != TokenType.NUMBER && valTok.getType() != TokenType.IDENTIFIER) {
                throw new IllegalArgumentException("Expected literal, found " + valTok);
            }
            whereClause = new WhereClause(col, "=", valTok.getValue());
        }

        return new UpdateStatement(tableName, setClauses, whereClause);
    }

    private DeleteStatement parseDelete() {
        consume(TokenType.DELETE, "Expected DELETE");
        consume(TokenType.FROM, "Expected FROM");
        String tableName = consume(TokenType.IDENTIFIER, "Expected table name").getValue();

        // Optional WHERE
        WhereClause whereClause = null;
        if (peek().getType() == TokenType.WHERE) {
            consume(TokenType.WHERE, "Expected WHERE");
            String col = consume(TokenType.IDENTIFIER, "Expected column name").getValue();
            consume(TokenType.EQUALS, "Expected '='");
            Token valTok = next();
            if (valTok.getType() != TokenType.STRING && valTok.getType() != TokenType.NUMBER && valTok.getType() != TokenType.IDENTIFIER) {
                throw new IllegalArgumentException("Expected literal, found " + valTok);
            }
            whereClause = new WhereClause(col, "=", valTok.getValue());
        }

        return new DeleteStatement(tableName, whereClause);
    }
}
