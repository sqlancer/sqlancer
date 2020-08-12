package sqlancer.cockroachdb.ast;

import sqlancer.common.visitor.UnaryOperation;

public class CockroachDBOrderingTerm implements UnaryOperation<CockroachDBExpression>, CockroachDBExpression {

    private final CockroachDBExpression expr;
    private final boolean asc;

    public CockroachDBOrderingTerm(CockroachDBExpression expr, boolean asc) {
        this.expr = expr;
        this.asc = asc;
    }

    @Override
    public CockroachDBExpression getExpression() {
        return expr;
    }

    @Override
    public String getOperatorRepresentation() {
        return asc ? "ASC" : "DESC";
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
