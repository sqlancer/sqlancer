package sqlancer.cnosdb.ast;

import sqlancer.LikeImplementationHelper;
import sqlancer.cnosdb.CnosDBSchema.CnosDBDataType;
import sqlancer.common.ast.BinaryNode;

public class CnosDBLikeOperation extends BinaryNode<CnosDBExpression> implements CnosDBExpression {

    public CnosDBLikeOperation(CnosDBExpression left, CnosDBExpression right) {
        super(left, right);
    }

    @Override
    public CnosDBDataType getExpressionType() {
        return CnosDBDataType.BOOLEAN;
    }

    @Override
    public CnosDBConstant getExpectedValue() {
        CnosDBConstant leftVal = getLeft().getExpectedValue();
        CnosDBConstant rightVal = getRight().getExpectedValue();
        if (leftVal == null || rightVal == null) {
            return null;
        }
        if (leftVal.isNull() || rightVal.isNull()) {
            return CnosDBConstant.createNullConstant();
        } else {
            boolean val = LikeImplementationHelper.match(leftVal.asString(), rightVal.asString(), 0, 0, true);
            return CnosDBConstant.createBooleanConstant(val);
        }
    }

    @Override
    public String getOperatorRepresentation() {
        return "LIKE";
    }

}
