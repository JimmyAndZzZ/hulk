package com.jimmy.hulk.protocol.utils.parse;

import cn.hutool.core.util.StrUtil;

public final class ShowParse {

    public static final int OTHER = -1;
    public static final int DATABASES = 1;
    public static final int DATASOURCES = 2;
    public static final int COLLATION = 3;
    public static final int FULL_TABLES = 4;
    public static final int FULL_COLUMNS = 5;
    public static final int KEYS = 6;
    public static final int VARIABLES = 7;
    public static final int SHOW_TABLES = 8;
    public static final int SHOW_CREATE_TABLE = 9;
    public static final int SHOW_TABLE_STATUS = 10;
    public static final int COLUMNS = 11;
    public static final int SHOW_TABLE_STATUS_LIKE = 12;
    public static final int SHOW_STATUS = 13;
    public static final int SHOW_HULK_DATABASES = 14;

    public static int parse(String stmt, int offset) {
        int i = offset;
        for (; i < stmt.length(); i++) {
            switch (stmt.charAt(i)) {
                case ' ':
                    continue;
                case '/':
                case '#':
                    i = ParseUtil.comment(stmt, i);
                    continue;
                case 'C':
                case 'c':
                    if (stmt.trim().toUpperCase().startsWith("SHOW CREATE TABLE")) {
                        return SHOW_CREATE_TABLE;
                    }

                    if (stmt.trim().toUpperCase().startsWith("SHOW COLUMNS FROM")) {
                        return COLUMNS;
                    }
                    return COLLATION;
                case 'D':
                case 'd':
                    return dataCheck(stmt, i);
                case 'F':
                case 'f':
                    if (stmt.trim().startsWith("SHOW FULL TABLES")) {
                        return FULL_TABLES;
                    } else if (stmt.trim().startsWith("SHOW FULL COLUMNS")) {
                        return FULL_COLUMNS;
                    }
                    return OTHER;
                case 'K':
                case 'k':
                    return KEYS;
                case 'V':
                case 'v':
                    if (stmt.trim().toUpperCase().startsWith("SHOW VARIABLES")) {
                        return VARIABLES;
                    }
                    return OTHER;
                case 'T':
                case 't':
                    if ("SHOW STATUS".equalsIgnoreCase(stmt.trim())) {
                        return SHOW_STATUS;
                    }

                    if ("SHOW TABLE STATUS".equalsIgnoreCase(stmt.trim())) {
                        return SHOW_TABLE_STATUS;
                    }

                    if (StrUtil.startWith(stmt.trim(), "SHOW TABLE STATUS") && StrUtil.containsAnyIgnoreCase(stmt.trim(), "LIKE")) {
                        return SHOW_TABLE_STATUS_LIKE;
                    }

                    if (stmt.trim().toUpperCase().startsWith("SHOW TABLES")) {
                        return SHOW_TABLES;
                    }

                    return OTHER;
                case 'H':
                case 'h':
                    if ("SHOW HULK DATABASES".equalsIgnoreCase(stmt.trim())) {
                        return SHOW_HULK_DATABASES;
                    }

                    return OTHER;
                default:
                    return OTHER;
            }
        }
        return OTHER;
    }

    // SHOW DATA
    static int dataCheck(String stmt, int offset) {
        if (stmt.length() > offset + "ata?".length()) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            if ((c1 == 'A' || c1 == 'a') && (c2 == 'T' || c2 == 't') && (c3 == 'A' || c3 == 'a')) {
                switch (stmt.charAt(++offset)) {
                    case 'B':
                    case 'b':
                        return showDatabases(stmt, offset);
                    case 'S':
                    case 's':
                        return showDataSources(stmt, offset);
                    default:
                        return OTHER;
                }
            }
        }
        return OTHER;
    }

    // SHOW DATABASES
    static int showDatabases(String stmt, int offset) {
        if (stmt.length() > offset + "ases".length()) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            if ((c1 == 'A' || c1 == 'a') && (c2 == 'S' || c2 == 's') && (c3 == 'E' || c3 == 'e')
                    && (c4 == 'S' || c4 == 's') && (stmt.length() == ++offset || ParseUtil
                    .isEOF(stmt.charAt(offset)))) {
                return DATABASES;
            }
        }
        return OTHER;
    }

    // SHOW DATASOURCES
    static int showDataSources(String stmt, int offset) {
        if (stmt.length() > offset + "ources".length()) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            char c6 = stmt.charAt(++offset);
            if ((c1 == 'O' || c1 == 'o') && (c2 == 'U' || c2 == 'u') && (c3 == 'R' || c3 == 'r')
                    && (c4 == 'C' || c4 == 'c') && (c5 == 'E' || c5 == 'e') && (c6 == 'S' || c6 == 's')
                    && (stmt.length() == ++offset || ParseUtil.isEOF(stmt.charAt(offset)))) {
                return DATASOURCES;
            }
        }
        return OTHER;
    }

}