package sqlancer.sqlite3.ast;

import sqlancer.sqlite3.schema.SQLite3Schema;

public class SQLite3BetweenOperation implements SQLite3Expression {

    private final SQLite3Expression expr;
    private final boolean negated;
    private final SQLite3Expression left;
    private final SQLite3Expression right;

    public SQLite3BetweenOperation(SQLite3Expression expr, boolean negated, SQLite3Expression left,
            SQLite3Expression right) {
        this.expr = expr;
        this.negated = negated;
        this.left = left;
        this.right = right;
    }

    public SQLite3Expression getExpression() {
        return expr;
    }

    public boolean isNegated() {
        return negated;
    }

    public SQLite3Expression getLeft() {
        return left;
    }

    public SQLite3Expression getRight() {
        return right;
    }

    @Override
    public SQLite3Schema.SQLite3Column.SQLite3CollateSequence getExplicitCollateSequence() {
        if (expr.getExplicitCollateSequence() != null) {
            return expr.getExplicitCollateSequence();
        } else if (left.getExplicitCollateSequence() != null) {
            return left.getExplicitCollateSequence();
        } else {
            return right.getExplicitCollateSequence();
        }
    }

    @Override
    public SQLite3Constant getExpectedValue() {
        return getTopNode().getExpectedValue();
    }

    public SQLite3Expression getTopNode() {
        SQLite3BinaryComparisonOperation leftOp = new SQLite3BinaryComparisonOperation(expr, left,
                SQLite3BinaryComparisonOperation.BinaryComparisonOperator.GREATER_EQUALS);
        SQLite3BinaryComparisonOperation rightOp = new SQLite3BinaryComparisonOperation(expr, right,
                SQLite3BinaryComparisonOperation.BinaryComparisonOperator.SMALLER_EQUALS);
        SQLite3BinaryOperation and = new SQLite3BinaryOperation(leftOp, rightOp,
                SQLite3BinaryOperation.BinaryOperator.AND);
        if (negated) {
            return new SQLite3UnaryOperation(SQLite3UnaryOperation.UnaryOperator.NOT, and);
        } else {
            return and;
        }
    }

}
