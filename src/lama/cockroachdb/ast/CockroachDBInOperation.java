package lama.cockroachdb.ast;

import java.util.List;

public class CockroachDBInOperation extends CockroachDBExpression {
	
	private final CockroachDBExpression left;
	private final List<CockroachDBExpression> right;

	public CockroachDBInOperation(CockroachDBExpression left, List<CockroachDBExpression> right) {
		this.left = left;
		this.right = right;
	}

	public CockroachDBExpression getLeft() {
		return left;
	}
	
	public List<CockroachDBExpression> getRight() {
		return right;
	}
	
}
