package sqlancer.postgres.ast;

import sqlancer.common.visitor.UnaryOperation;

public class PostgresAlias implements UnaryOperation<PostgresExpression>, PostgresExpression {

    private final PostgresExpression expr;
    private final String alias;

    public PostgresAlias(PostgresExpression expr, String alias) {
        this.expr = expr;
        this.alias = alias;
    }

    @Override
    public PostgresExpression getExpression() {
        return expr;
    }

    @Override
    public String getOperatorRepresentation() {
        return " as " + alias;
    }

    @Override
    public OperatorKind getOperatorKind() {
        return OperatorKind.POSTFIX;
    }

    @Override
    public boolean omitBracketsWhenPrinting() {
        return true;
    }

}
