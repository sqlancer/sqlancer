package sqlancer.yugabyte.ysql.ast;

import sqlancer.common.ast.BinaryNode;
import sqlancer.yugabyte.ysql.YSQLSchema.YSQLDataType;

public class YSQLConcatOperation extends BinaryNode<YSQLExpression> implements YSQLExpression {

    public YSQLConcatOperation(YSQLExpression left, YSQLExpression right) {
        super(left, right);
    }

    @Override
    public YSQLDataType getExpressionType() {
        return YSQLDataType.TEXT;
    }

    @Override
    public YSQLConstant getExpectedValue() {
        YSQLConstant leftExpectedValue = getLeft().getExpectedValue();
        YSQLConstant rightExpectedValue = getRight().getExpectedValue();
        if (leftExpectedValue == null || rightExpectedValue == null) {
            return null;
        }
        if (leftExpectedValue.isNull() || rightExpectedValue.isNull()) {
            return YSQLConstant.createNullConstant();
        }
        String leftStr = leftExpectedValue.cast(YSQLDataType.TEXT).getUnquotedTextRepresentation();
        String rightStr = rightExpectedValue.cast(YSQLDataType.TEXT).getUnquotedTextRepresentation();
        return YSQLConstant.createTextConstant(leftStr + rightStr);
    }

    @Override
    public String getOperatorRepresentation() {
        return "||";
    }

}
