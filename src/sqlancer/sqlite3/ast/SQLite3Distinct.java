package sqlancer.sqlite3.ast;

import sqlancer.sqlite3.schema.SQLite3Schema;

public class SQLite3Distinct implements SQLite3Expression {

    private final SQLite3Expression expr;

    public SQLite3Distinct(SQLite3Expression expr) {
        this.expr = expr;
    }

    @Override
    public SQLite3Schema.SQLite3Column.SQLite3CollateSequence getExplicitCollateSequence() {
        return expr.getExplicitCollateSequence();
    }

    @Override
    public SQLite3Constant getExpectedValue() {
        return expr.getExpectedValue();
    }

    public SQLite3Expression getExpression() {
        return expr;
    }

    @Override
    public SQLite3Schema.SQLite3Column.SQLite3CollateSequence getImplicitCollateSequence() {
        // https://www.sqlite.org/src/tktview/18ab5da2c05ad57d7f9d79c41d3138b141378543
        return expr.getImplicitCollateSequence();
    }

}
