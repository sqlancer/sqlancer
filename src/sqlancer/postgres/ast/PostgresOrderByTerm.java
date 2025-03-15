package sqlancer.postgres.ast;

import sqlancer.Randomly;
import sqlancer.postgres.PostgresSchema.PostgresDataType;

public class PostgresOrderByTerm implements PostgresExpression {

    private final PostgresExpression expr;
    private final PostgresOrder order;

    public enum PostgresOrder {
        ASC, DESC;

        public static PostgresOrder getRandomOrder() {
            return Randomly.fromOptions(PostgresOrder.values());
        }
    }

    public PostgresOrderByTerm(PostgresExpression expr, PostgresOrder order) {
        if (expr == null) {
            throw new IllegalArgumentException("Expression cannot be null");
        }
        this.expr = expr;
        this.order = order;
    }

    // Constructor for window functions, might be removed in the future to have only one constructor
    public PostgresOrderByTerm(PostgresExpression expr, boolean ascending) {
        if (expr == null) {
            throw new IllegalArgumentException("Expression cannot be null");
        }
        this.expr = expr;
        this.order = ascending ? PostgresOrder.ASC : PostgresOrder.DESC;
    }

    public PostgresExpression getExpr() {
        return expr;
    }

    public PostgresOrder getOrder() {
        return order;
    }

    public boolean isAscending() {
        return order == PostgresOrder.ASC;
    }

    @Override
    public PostgresConstant getExpectedValue() {
        throw new AssertionError(this);
    }

    @Override
    public PostgresDataType getExpressionType() {
        return null;
    }

    @Override
    public String toString() {
        return String.format("%s %s", expr, order);
    }
}
