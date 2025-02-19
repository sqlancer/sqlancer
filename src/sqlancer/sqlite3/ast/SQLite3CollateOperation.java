package sqlancer.sqlite3.ast;

import sqlancer.sqlite3.schema.SQLite3Schema.SQLite3Column.SQLite3CollateSequence;

public class SQLite3CollateOperation extends SQLite3Expression {

    private final SQLite3Expression expression;
    private final SQLite3CollateSequence collate;

    public SQLite3CollateOperation(SQLite3Expression expression, SQLite3CollateSequence collate) {
        this.expression = expression;
        this.collate = collate;
    }

    public SQLite3CollateSequence getCollate() {
        return collate;
    }

    public SQLite3Expression getExpression() {
        return expression;
    }

    // If either operand has an explicit collating function assignment using the
    // postfix COLLATE operator, then the explicit collating function is used for
    // comparison, with precedence to the collating function of the left operand.
    @Override
    public SQLite3CollateSequence getExplicitCollateSequence() {
        return collate;
    }

    @Override
    public SQLite3Constant getExpectedValue() {
        return expression.getExpectedValue();
    }

    @Override
    public TypeAffinity getAffinity() {
        return expression.getAffinity();
    }

}
