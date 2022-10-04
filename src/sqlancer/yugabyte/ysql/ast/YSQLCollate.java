package sqlancer.yugabyte.ysql.ast;

import sqlancer.yugabyte.ysql.YSQLSchema.YSQLDataType;

public class YSQLCollate implements YSQLExpression {

    private final YSQLExpression expr;
    private final String collate;

    public YSQLCollate(YSQLExpression expr, String collate) {
        this.expr = expr;
        this.collate = collate;
    }

    public String getCollate() {
        return collate;
    }

    public YSQLExpression getExpr() {
        return expr;
    }

    @Override
    public YSQLDataType getExpressionType() {
        return expr.getExpressionType();
    }

    @Override
    public YSQLConstant getExpectedValue() {
        return null;
    }

}
