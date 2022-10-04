package sqlancer.yugabyte.ysql.ast;

import sqlancer.yugabyte.ysql.YSQLSchema.YSQLColumn;
import sqlancer.yugabyte.ysql.YSQLSchema.YSQLDataType;

public class YSQLColumnValue implements YSQLExpression {

    private final YSQLColumn c;
    private final YSQLConstant expectedValue;

    public YSQLColumnValue(YSQLColumn c, YSQLConstant expectedValue) {
        this.c = c;
        this.expectedValue = expectedValue;
    }

    public static YSQLColumnValue create(YSQLColumn c, YSQLConstant expected) {
        return new YSQLColumnValue(c, expected);
    }

    @Override
    public YSQLDataType getExpressionType() {
        return c.getType();
    }

    @Override
    public YSQLConstant getExpectedValue() {
        return expectedValue;
    }

    public YSQLColumn getColumn() {
        return c;
    }

}
