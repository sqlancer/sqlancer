package sqlancer.materialize.ast;

import sqlancer.Randomly;
import sqlancer.materialize.MaterializeSchema.MaterializeDataType;

public class MaterializeOrderByTerm implements MaterializeExpression {

    private final MaterializeOrder order;
    private final MaterializeExpression expr;

    public enum MaterializeOrder {
        ASC, DESC;

        public static MaterializeOrder getRandomOrder() {
            return Randomly.fromOptions(MaterializeOrder.values());
        }
    }

    public MaterializeOrderByTerm(MaterializeExpression expr, MaterializeOrder order) {
        this.expr = expr;
        this.order = order;
    }

    public MaterializeOrder getOrder() {
        return order;
    }

    public MaterializeExpression getExpr() {
        return expr;
    }

    @Override
    public MaterializeConstant getExpectedValue() {
        throw new AssertionError(this);
    }

    @Override
    public MaterializeDataType getExpressionType() {
        return null;
    }

}
