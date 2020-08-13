package sqlancer.tidb.ast;

import sqlancer.common.visitor.UnaryOperation;

public class TiDBOrderingTerm implements UnaryOperation<TiDBExpression>, TiDBExpression {

    private final TiDBExpression expr;
    private final boolean asc;

    public TiDBOrderingTerm(TiDBExpression expr, boolean asc) {
        this.expr = expr;
        this.asc = asc;
    }

    @Override
    public TiDBExpression getExpression() {
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
