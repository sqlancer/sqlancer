package sqlancer.postgres.ast;

import sqlancer.Randomly;
import sqlancer.postgres.PostgresSchema.PostgresDataType;

public class PostgresOrderByTerm implements PostgresExpression {

    private final PostgresOrder order;
    private final PostgresExpression expr;

    public enum PostgresOrder {
        ASC, DESC;

        public static PostgresOrder getRandomOrder() {
            return Randomly.fromOptions(PostgresOrder.values());
        }
    }

    public PostgresOrderByTerm(PostgresExpression expr, PostgresOrder order) {
        this.expr = expr;
        this.order = order;
    }

    public PostgresOrder getOrder() {
        return order;
    }

    public PostgresExpression getExpr() {
        return expr;
    }

    @Override
    public PostgresConstant getExpectedValue() {
        throw new AssertionError(this);
    }

    @Override
    public PostgresDataType getExpressionType() {
        return null;
    }

}
