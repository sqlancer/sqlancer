package sqlancer.postgres.ast;

import sqlancer.common.ast.BinaryNode;
import sqlancer.postgres.PostgresSchema.PostgresDataType;

public class PostgresConcatOperation extends BinaryNode<PostgresExpression> implements PostgresExpression {

    public PostgresConcatOperation(PostgresExpression left, PostgresExpression right) {
        super(left, right);
    }

    @Override
    public PostgresDataType getExpressionType() {
        return PostgresDataType.TEXT;
    }

    @Override
    public PostgresConstant getExpectedValue() {
        PostgresConstant leftExpectedValue = getLeft().getExpectedValue();
        PostgresConstant rightExpectedValue = getRight().getExpectedValue();
        if (leftExpectedValue == null || rightExpectedValue == null) {
            return null;
        }
        if (leftExpectedValue.isNull() || rightExpectedValue.isNull()) {
            return PostgresConstant.createNullConstant();
        }
        String leftStr = leftExpectedValue.cast(PostgresDataType.TEXT).getUnquotedTextRepresentation();
        String rightStr = rightExpectedValue.cast(PostgresDataType.TEXT).getUnquotedTextRepresentation();
        return PostgresConstant.createTextConstant(leftStr + rightStr);
    }

    @Override
    public String getOperatorRepresentation() {
        return "||";
    }

}
