package sqlancer.cnosdb.ast;

import sqlancer.cnosdb.CnosDBSchema.CnosDBDataType;
import sqlancer.common.ast.BinaryNode;

public class CnosDBConcatOperation extends BinaryNode<CnosDBExpression> implements CnosDBExpression {

    public CnosDBConcatOperation(CnosDBExpression left, CnosDBExpression right) {
        super(left, right);
    }

    @Override
    public CnosDBDataType getExpressionType() {
        return CnosDBDataType.STRING;
    }

    @Override
    public CnosDBConstant getExpectedValue() {
        CnosDBConstant leftExpectedValue = getLeft().getExpectedValue();
        CnosDBConstant rightExpectedValue = getRight().getExpectedValue();
        if (leftExpectedValue == null || rightExpectedValue == null) {
            return null;
        }
        if (leftExpectedValue.isNull() || rightExpectedValue.isNull()) {
            return CnosDBConstant.createNullConstant();
        }
        String leftStr = leftExpectedValue.cast(CnosDBDataType.STRING).getUnquotedTextRepresentation();
        String rightStr = rightExpectedValue.cast(CnosDBDataType.STRING).getUnquotedTextRepresentation();
        return CnosDBConstant.createStringConstant(leftStr + rightStr);
    }

    @Override
    public String getOperatorRepresentation() {
        return "||";
    }

}
