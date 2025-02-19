package sqlancer.sqlite3.ast;

/**
 * Represents SQLite type affinities. See https://www.sqlite.org/datatype3.html 3.2
 */
public enum SQLite3TypeAffinity {
    INTEGER, TEXT, BLOB, REAL, NUMERIC, NONE;

    public boolean isNumeric() {
        return this == INTEGER || this == REAL || this == NUMERIC;
    }
}