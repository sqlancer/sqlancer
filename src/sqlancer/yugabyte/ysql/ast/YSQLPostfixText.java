package sqlancer.yugabyte.ysql.ast;

import sqlancer.yugabyte.ysql.YSQLSchema.YSQLDataType;

public class YSQLPostfixText implements YSQLExpression {

    private final YSQLExpression expr;
    private final String text;
    private final YSQLConstant expectedValue;
    private final YSQLDataType type;

    public YSQLPostfixText(YSQLExpression expr, String text, YSQLConstant expectedValue, YSQLDataType type) {
        this.expr = expr;
        this.text = text;
        this.expectedValue = expectedValue;
        this.type = type;
    }

    public YSQLExpression getExpr() {
        return expr;
    }

    public String getText() {
        return text;
    }

    @Override
    public YSQLDataType getExpressionType() {
        return type;
    }

    @Override
    public YSQLConstant getExpectedValue() {
        return expectedValue;
    }
}
