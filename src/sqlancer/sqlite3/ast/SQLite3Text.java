package sqlancer.sqlite3.ast;

import sqlancer.sqlite3.schema.SQLite3Schema;

public class SQLite3Text implements SQLite3Expression {

    private final String text;
    private final SQLite3Constant expectedValue;

    public SQLite3Text(String text, SQLite3Constant expectedValue) {
        this.text = text;
        this.expectedValue = expectedValue;
    }

    public String getText() {
        return text;
    }

    @Override
    public SQLite3Schema.SQLite3Column.SQLite3CollateSequence getExplicitCollateSequence() {
        return null;
    }

    @Override
    public SQLite3Constant getExpectedValue() {
        return expectedValue;
    }

}
