package sqlancer.cockroachdb.ast;

import sqlancer.common.visitor.UnaryOperation;

public class CockroachDBAlias implements UnaryOperation<CockroachDBExpression>, CockroachDBExpression {

    private final CockroachDBExpression expr;
    private final String alias;

    public CockroachDBAlias(CockroachDBExpression expr, String alias) {
        this.expr = expr;
        this.alias = alias;
    }

    @Override
    public CockroachDBExpression getExpression() {
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
