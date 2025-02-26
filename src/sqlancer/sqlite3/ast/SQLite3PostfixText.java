package sqlancer.sqlite3.ast;

import sqlancer.common.visitor.UnaryOperation;
import sqlancer.sqlite3.schema.SQLite3Schema;

public class SQLite3PostfixText implements SQLite3Expression, UnaryOperation<SQLite3Expression> {

    private final SQLite3Expression expr;
    private final String text;
    private SQLite3Constant expectedValue;

    public SQLite3PostfixText(SQLite3Expression expr, String text, SQLite3Constant expectedValue) {
        this.expr = expr;
        this.text = text;
        this.expectedValue = expectedValue;
    }

    public SQLite3PostfixText(String text, SQLite3Constant expectedValue) {
        this(null, text, expectedValue);
    }

    public String getText() {
        return text;
    }

    @Override
    public SQLite3Schema.SQLite3Column.SQLite3CollateSequence getExplicitCollateSequence() {
        if (expr == null) {
            return null;
        } else {
            return expr.getExplicitCollateSequence();
        }
    }

    @Override
    public SQLite3Constant getExpectedValue() {
        return expectedValue;
    }

    @Override
    public SQLite3Expression getExpression() {
        return expr;
    }

    @Override
    public String getOperatorRepresentation() {
        return getText();
    }

    @Override
    public OperatorKind getOperatorKind() {
        return OperatorKind.POSTFIX;
    }

    @Override
    public boolean omitBracketsWhenPrinting() {
        return true;
    }
}
