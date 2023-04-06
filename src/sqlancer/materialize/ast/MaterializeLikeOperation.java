package sqlancer.materialize.ast;

import sqlancer.LikeImplementationHelper;
import sqlancer.common.ast.BinaryNode;
import sqlancer.materialize.MaterializeSchema.MaterializeDataType;

public class MaterializeLikeOperation extends BinaryNode<MaterializeExpression> implements MaterializeExpression {

    public MaterializeLikeOperation(MaterializeExpression left, MaterializeExpression right) {
        super(left, right);
    }

    @Override
    public MaterializeDataType getExpressionType() {
        return MaterializeDataType.BOOLEAN;
    }

    @Override
    public MaterializeConstant getExpectedValue() {
        MaterializeConstant leftVal = getLeft().getExpectedValue();
        MaterializeConstant rightVal = getRight().getExpectedValue();
        if (leftVal == null || rightVal == null) {
            return null;
        }
        if (leftVal.isNull() || rightVal.isNull()) {
            return MaterializeConstant.createNullConstant();
        } else {
            boolean val = LikeImplementationHelper.match(leftVal.asString(), rightVal.asString(), 0, 0, true);
            return MaterializeConstant.createBooleanConstant(val);
        }
    }

    @Override
    public String getOperatorRepresentation() {
        return "LIKE";
    }

}
