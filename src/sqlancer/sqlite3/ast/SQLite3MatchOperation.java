package sqlancer.sqlite3.ast;

import sqlancer.sqlite3.schema.SQLite3Schema;

public class SQLite3MatchOperation implements SQLite3Expression {

    private final SQLite3Expression left;
    private final SQLite3Expression right;

    public SQLite3MatchOperation(SQLite3Expression left, SQLite3Expression right) {
        this.left = left;
        this.right = right;
    }

    @Override
    public SQLite3Schema.SQLite3Column.SQLite3CollateSequence getExplicitCollateSequence() {
        return null;
    }

    public SQLite3Expression getLeft() {
        return left;
    }

    public SQLite3Expression getRight() {
        return right;
    }

}