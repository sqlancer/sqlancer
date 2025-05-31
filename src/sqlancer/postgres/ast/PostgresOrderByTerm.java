package sqlancer.postgres.ast;

import sqlancer.Randomly;
import sqlancer.postgres.PostgresSchema.PostgresDataType;

public class PostgresOrderByTerm implements PostgresExpression {

    private final PostgresExpression expr;
    private final PostgresOrder order;
    private final int limit;
    private final boolean ties;

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

        if (Randomly.getBooleanWithRatherLowProbability()) {
            this.limit = (int) Randomly.getPositiveOrZeroNonCachedInteger();
            this.ties = true;
        } else {
            this.limit = 0;
            this.ties = false;
        }

    }

    // Constructor for window functions, might be removed in the future to have only one constructor
    public PostgresOrderByTerm(PostgresExpression expr, boolean ascending) {
        if (expr == null) {
            throw new IllegalArgumentException("Expression cannot be null");
        }
        this.expr = expr;
        this.order = ascending ? PostgresOrder.ASC : PostgresOrder.DESC;
        this.limit = 0;
        this.ties = false;
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
        if (ties) {
            return String.format("%s %s FETCH FIRST %d WITH TIES", expr, order, limit);
        } else {
            return String.format("%s %s", expr, order);
        }
    }
}
