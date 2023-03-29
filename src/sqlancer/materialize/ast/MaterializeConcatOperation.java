package sqlancer.materialize.ast;

import sqlancer.common.ast.BinaryNode;
import sqlancer.materialize.MaterializeSchema.MaterializeDataType;

public class MaterializeConcatOperation extends BinaryNode<MaterializeExpression> implements MaterializeExpression {

    public MaterializeConcatOperation(MaterializeExpression left, MaterializeExpression right) {
        super(left, right);
    }

    @Override
    public MaterializeDataType getExpressionType() {
        return MaterializeDataType.TEXT;
    }

    @Override
    public MaterializeConstant getExpectedValue() {
        MaterializeConstant leftExpectedValue = getLeft().getExpectedValue();
        MaterializeConstant rightExpectedValue = getRight().getExpectedValue();
        if (leftExpectedValue == null || rightExpectedValue == null) {
            return null;
        }
        if (leftExpectedValue.isNull() || rightExpectedValue.isNull()) {
            return MaterializeConstant.createNullConstant();
        }
        String leftStr = leftExpectedValue.cast(MaterializeDataType.TEXT).getUnquotedTextRepresentation();
        String rightStr = rightExpectedValue.cast(MaterializeDataType.TEXT).getUnquotedTextRepresentation();
        return MaterializeConstant.createTextConstant(leftStr + rightStr);
    }

    @Override
    public String getOperatorRepresentation() {
        return "||";
    }

}
