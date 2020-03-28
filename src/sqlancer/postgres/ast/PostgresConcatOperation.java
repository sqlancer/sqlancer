package sqlancer.postgres.ast;

import sqlancer.ast.BinaryNode;
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
		if (getLeft().getExpectedValue().isNull() || getRight().getExpectedValue().isNull()) {
			return PostgresConstant.createNullConstant();
		}
		String leftStr = getLeft().getExpectedValue().cast(PostgresDataType.TEXT).getUnquotedTextRepresentation();
		String rightStr = getRight().getExpectedValue().cast(PostgresDataType.TEXT).getUnquotedTextRepresentation();
		return PostgresConstant.createTextConstant(leftStr + rightStr);
	}

	@Override
	public String getOperatorRepresentation() {
		return "||";
	}

}
