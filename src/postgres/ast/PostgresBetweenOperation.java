package postgres.ast;


import postgres.PostgresSchema.PostgresDataType;
import postgres.ast.PostgresBinaryComparisonOperation.PostgresBinaryComparisonOperator;

public class PostgresBetweenOperation extends PostgresExpression {

	private final PostgresExpression expr;
	private final PostgresExpression left;
	private final PostgresExpression right;

	public PostgresBetweenOperation(PostgresExpression expr, PostgresExpression left, PostgresExpression right) {
		this.expr = expr;
		this.left = left;
		this.right = right;
	}

	public PostgresExpression getExpr() {
		return expr;
	}

	public PostgresExpression getLeft() {
		return left;
	}

	public PostgresExpression getRight() {
		return right;
	}

	@Override
	public PostgresConstant getExpectedValue() {
		PostgresBinaryComparisonOperation leftComparison = new PostgresBinaryComparisonOperation(left, expr,
				PostgresBinaryComparisonOperator.LESS_EQUALS);
		PostgresBinaryComparisonOperation rightComparison = new PostgresBinaryComparisonOperation(expr, right,
				PostgresBinaryComparisonOperator.LESS_EQUALS);
		PostgresBinaryLogicalOperation andOperation = new PostgresBinaryLogicalOperation(leftComparison,
				rightComparison, PostgresBinaryLogicalOperation.BinaryLogicalOperator.AND);
		return andOperation.getExpectedValue();
	}

	@Override
	public PostgresDataType getExpressionType() {
		return PostgresDataType.BOOLEAN;
	}

}
