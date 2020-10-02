package sqlancer.postgres.ast;

import sqlancer.postgres.PostgresSchema.PostgresColumn;
import sqlancer.postgres.PostgresSchema.PostgresDataType;

public class PostgresColumnValue implements PostgresExpression {

    private final PostgresColumn c;
    private final PostgresConstant expectedValue;

    public PostgresColumnValue(PostgresColumn c, PostgresConstant expectedValue) {
        this.c = c;
        this.expectedValue = expectedValue;
    }

    @Override
    public PostgresDataType getExpressionType() {
        return c.getType();
    }

    @Override
    public PostgresConstant getExpectedValue() {
        return expectedValue;
    }

    public static PostgresColumnValue create(PostgresColumn c, PostgresConstant expected) {
        return new PostgresColumnValue(c, expected);
    }

    public PostgresColumn getColumn() {
        return c;
    }

}
