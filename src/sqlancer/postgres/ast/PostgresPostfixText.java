package sqlancer.postgres.ast;

import sqlancer.postgres.PostgresSchema.PostgresDataType;

public class PostgresPostfixText implements PostgresExpression {

    private final PostgresExpression expr;
    private final String text;
    private final PostgresConstant expectedValue;
    private final PostgresDataType type;

    public PostgresPostfixText(PostgresExpression expr, String text, PostgresConstant expectedValue,
            PostgresDataType type) {
        this.expr = expr;
        this.text = text;
        this.expectedValue = expectedValue;
        this.type = type;
    }

    public PostgresExpression getExpr() {
        return expr;
    }

    public String getText() {
        return text;
    }

    @Override
    public PostgresConstant getExpectedValue() {
        return expectedValue;
    }

    @Override
    public PostgresDataType getExpressionType() {
        return type;
    }
}
