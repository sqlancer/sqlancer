package sqlancer.materialize.ast;

import sqlancer.materialize.MaterializeSchema.MaterializeDataType;

public class MaterializeCollate implements MaterializeExpression {

    private final MaterializeExpression expr;
    private final String collate;

    public MaterializeCollate(MaterializeExpression expr, String collate) {
        this.expr = expr;
        this.collate = collate;
    }

    public String getCollate() {
        return collate;
    }

    public MaterializeExpression getExpr() {
        return expr;
    }

    @Override
    public MaterializeDataType getExpressionType() {
        return expr.getExpressionType();
    }

    @Override
    public MaterializeConstant getExpectedValue() {
        return null;
    }

}
