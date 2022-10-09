package sqlancer.yugabyte.ysql.ast;

import sqlancer.common.visitor.UnaryOperation;

public class YSQLAlias implements UnaryOperation<YSQLExpression>, YSQLExpression {

    private final YSQLExpression expr;
    private final String alias;

    public YSQLAlias(YSQLExpression expr, String alias) {
        this.expr = expr;
        this.alias = alias;
    }

    @Override
    public YSQLExpression getExpression() {
        return expr;
    }

    @Override
    public String getOperatorRepresentation() {
        return " as " + alias;
    }

    @Override
    public boolean omitBracketsWhenPrinting() {
        return true;
    }

    @Override
    public OperatorKind getOperatorKind() {
        return OperatorKind.POSTFIX;
    }

}
