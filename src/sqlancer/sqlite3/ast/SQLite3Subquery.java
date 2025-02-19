package sqlancer.sqlite3.ast;

import sqlancer.sqlite3.schema.SQLite3Schema.SQLite3Column.SQLite3CollateSequence;

public class SQLite3Subquery implements SQLite3Expression {
    private final String query;

    public SQLite3Subquery(String query) {
        this.query = query;
    }

    public static SQLite3Expression create(String query) {
        return new SQLite3Subquery(query);
    }

    public String getQuery() {
        return query;
    }

    @Override
    public SQLite3CollateSequence getExplicitCollateSequence() {
        return null;
    }

    @Override
    public SQLite3Constant getExpectedValue() {
        return null;
    }

    @Override
    public SQLite3TypeAffinity getAffinity() {
        return SQLite3TypeAffinity.NONE;
    }
}