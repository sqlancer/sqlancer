package sqlancer.yugabyte.ysql.ast;

import sqlancer.Randomly;
import sqlancer.yugabyte.ysql.YSQLSchema.YSQLDataType;

public class YSQLOrderByTerm implements YSQLExpression {

    private final YSQLOrder order;
    private final YSQLExpression expr;

    public YSQLOrderByTerm(YSQLExpression expr, YSQLOrder order) {
        this.expr = expr;
        this.order = order;
    }

    public YSQLOrder getOrder() {
        return order;
    }

    public YSQLExpression getExpr() {
        return expr;
    }

    @Override
    public YSQLDataType getExpressionType() {
        return null;
    }

    @Override
    public YSQLConstant getExpectedValue() {
        throw new AssertionError(this);
    }

    public enum YSQLOrder {
        ASC, DESC;

        public static YSQLOrder getRandomOrder() {
            return Randomly.fromOptions(YSQLOrder.values());
        }
    }

}
