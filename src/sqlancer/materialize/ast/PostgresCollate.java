package sqlancer.postgres.ast;

import sqlancer.postgres.PostgresSchema.PostgresDataType;

public class PostgresCollate implements PostgresExpression {

    private final PostgresExpression expr;
    private final String collate;

    public PostgresCollate(PostgresExpression expr, String collate) {
        this.expr = expr;
        this.collate = collate;
    }

    public String getCollate() {
        return collate;
    }

    public PostgresExpression getExpr() {
        return expr;
    }

    @Override
    public PostgresDataType getExpressionType() {
        return expr.getExpressionType();
    }

    @Override
    public PostgresConstant getExpectedValue() {
        return null;
    }

}
