package com.atlasdb.sql.parser;

import java.util.*;

/**
 * Hand-written lexical analyzer (tokenizer) for AtlasDB SQL.
 */
public final class Lexer {

    private final String input;
    private int pos = 0;

    private static final Map<String, TokenType> KEYWORDS = new HashMap<>();

    static {
        KEYWORDS.put("create", TokenType.CREATE);
        KEYWORDS.put("table", TokenType.TABLE);
        KEYWORDS.put("insert", TokenType.INSERT);
        KEYWORDS.put("into", TokenType.INTO);
        KEYWORDS.put("values", TokenType.VALUES);
        KEYWORDS.put("select", TokenType.SELECT);
        KEYWORDS.put("from", TokenType.FROM);
        KEYWORDS.put("join", TokenType.JOIN);
        KEYWORDS.put("on", TokenType.ON);
        KEYWORDS.put("where", TokenType.WHERE);
        KEYWORDS.put("update", TokenType.UPDATE);
        KEYWORDS.put("set", TokenType.SET);
        KEYWORDS.put("delete", TokenType.DELETE);
        KEYWORDS.put("group", TokenType.GROUP);
        KEYWORDS.put("by", TokenType.BY);
        KEYWORDS.put("limit", TokenType.LIMIT);
        KEYWORDS.put("and", TokenType.AND);
        KEYWORDS.put("or", TokenType.OR);
        KEYWORDS.put("int", TokenType.INT);
        KEYWORDS.put("varchar", TokenType.VARCHAR);
    }

    public Lexer(String input) {
        this.input = input;
    }

    private char peek() {
        if (pos >= input.length()) {
            return '\0';
        }
        return input.charAt(pos);
    }

    private char next() {
        if (pos >= input.length()) {
            return '\0';
        }
        return input.charAt(pos++);
    }

    /**
     * Tokenizes the entire input string and returns the list of tokens.
     *
     * @return the list of parsed tokens
     */
    public List<Token> tokenize() {
        List<Token> tokens = new ArrayList<>();
        while (true) {
            Token t = nextToken();
            tokens.add(t);
            if (t.getType() == TokenType.EOF) {
                break;
            }
        }
        return tokens;
    }

    private Token nextToken() {
        skipWhitespace();

        if (pos >= input.length()) {
            return new Token(TokenType.EOF, "");
        }

        char c = peek();

        // Standard Symbols
        if (c == '(') {
            next();
            return new Token(TokenType.LPAREN, "(");
        }
        if (c == ')') {
            next();
            return new Token(TokenType.RPAREN, ")");
        }
        if (c == ',') {
            next();
            return new Token(TokenType.COMMA, ",");
        }
        if (c == '=') {
            next();
            return new Token(TokenType.EQUALS, "=");
        }
        if (c == '*') {
            next();
            return new Token(TokenType.STAR, "*");
        }
        if (c == '>') {
            next();
            return new Token(TokenType.GT, ">");
        }
        if (c == '<') {
            next();
            return new Token(TokenType.LT, "<");
        }

        // String Literals: enclosed in single quotes '
        if (c == '\'') {
            next(); // consume opening quote
            StringBuilder sb = new StringBuilder();
            while (pos < input.length() && peek() != '\'') {
                sb.append(next());
            }
            if (peek() == '\'') {
                next(); // consume closing quote
            } else {
                throw new IllegalArgumentException("Unterminated string literal at pos " + pos);
            }
            return new Token(TokenType.STRING, sb.toString());
        }

        // Numeric Literals
        if (Character.isDigit(c)) {
            StringBuilder sb = new StringBuilder();
            while (pos < input.length() && Character.isDigit(peek())) {
                sb.append(next());
            }
            return new Token(TokenType.NUMBER, sb.toString());
        }

        // Identifiers and Keywords
        if (Character.isLetter(c) || c == '_') {
            StringBuilder sb = new StringBuilder();
            // Identifiers can contain alphanumeric characters, underscores, and dots (for table.column notation)
            while (pos < input.length() && (Character.isLetterOrDigit(peek()) || peek() == '_' || peek() == '.')) {
                sb.append(next());
            }
            String val = sb.toString();
            TokenType type = KEYWORDS.get(val.toLowerCase());
            if (type != null) {
                return new Token(type, val);
            }
            return new Token(TokenType.IDENTIFIER, val);
        }

        throw new IllegalArgumentException("Unexpected character '" + c + "' at position " + pos);
    }

    private void skipWhitespace() {
        while (pos < input.length() && Character.isWhitespace(peek())) {
            pos++;
        }
    }
}
