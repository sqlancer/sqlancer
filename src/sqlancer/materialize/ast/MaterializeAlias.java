package sqlancer.materialize.ast;

import sqlancer.common.visitor.UnaryOperation;

public class MaterializeAlias implements UnaryOperation<MaterializeExpression>, MaterializeExpression {

    private final MaterializeExpression expr;
    private final String alias;

    public MaterializeAlias(MaterializeExpression expr, String alias) {
        this.expr = expr;
        this.alias = alias;
    }

    @Override
    public MaterializeExpression getExpression() {
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
