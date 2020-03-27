package sqlancer.cockroachdb.ast;

import sqlancer.visitor.BinaryOperation;

public class CockroachDBConcatOperation implements CockroachDBExpression, BinaryOperation<CockroachDBExpression>  {
	
	private final CockroachDBExpression left;
	private final CockroachDBExpression right;

	public CockroachDBConcatOperation(CockroachDBExpression left, CockroachDBExpression right) {
		this.left = left;
		this.right = right;
	}

	@Override
	public CockroachDBExpression getLeft() {
		return left;
	}

	@Override
	public CockroachDBExpression getRight() {
		return right;
	}

	@Override
	public String getOperatorRepresentation() {
		return "||";
	}

}
