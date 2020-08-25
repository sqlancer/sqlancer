package sqlancer.postgres.ast;

import sqlancer.LikeImplementationHelper;
import sqlancer.common.ast.BinaryNode;
import sqlancer.postgres.PostgresSchema.PostgresDataType;

public class PostgresLikeOperation extends BinaryNode<PostgresExpression> implements PostgresExpression {

    public PostgresLikeOperation(PostgresExpression left, PostgresExpression right) {
        super(left, right);
    }

    @Override
    public PostgresDataType getExpressionType() {
        return PostgresDataType.BOOLEAN;
    }

    @Override
    public PostgresConstant getExpectedValue() {
        PostgresConstant leftVal = getLeft().getExpectedValue();
        PostgresConstant rightVal = getRight().getExpectedValue();
        if (leftVal == null || rightVal == null) {
            return null;
        }
        if (leftVal.isNull() || rightVal.isNull()) {
            return PostgresConstant.createNullConstant();
        } else {
            boolean val = LikeImplementationHelper.match(leftVal.asString(), rightVal.asString(), 0, 0, true);
            return PostgresConstant.createBooleanConstant(val);
        }
    }

    @Override
    public String getOperatorRepresentation() {
        return "LIKE";
    }

}
