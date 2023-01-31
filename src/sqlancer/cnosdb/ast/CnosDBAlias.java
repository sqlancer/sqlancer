package sqlancer.cnosdb.ast;

import sqlancer.common.visitor.UnaryOperation;

public class CnosDBAlias implements UnaryOperation<CnosDBExpression>, CnosDBExpression {

    private final CnosDBExpression expr;
    private final String alias;

    public CnosDBAlias(CnosDBExpression expr, String alias) {
        this.expr = expr;
        this.alias = alias;
    }

    @Override
    public CnosDBExpression getExpression() {
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
