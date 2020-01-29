package lama.cockroachdb.ast;

import lama.Randomly;
import lama.visitor.BinaryOperation;

public class CockroachDBBinaryComparisonOperator extends CockroachDBExpression
		implements BinaryOperation<CockroachDBExpression> {

	public enum CockroachDBComparisonOperator {
		EQUALS("="), GREATER(">"), GREATER_EQUALS(">="), SMALLER("<"), SMALLER_EQUALS("<="), NOT_EQUALS("!="),
		IS_DISTINCT_FROM("IS DISTINCT FROM"), IS_NOT_DISTINCT_FROM("IS NOT DISTINCT FROM");

		private String textRepr;

		private CockroachDBComparisonOperator(String textRepr) {
			this.textRepr = textRepr;
		}

		public static CockroachDBComparisonOperator getRandom() {
			return Randomly.fromOptions(CockroachDBComparisonOperator.values());
		}

	}

	private final CockroachDBExpression left;
	private final CockroachDBExpression right;
	private final CockroachDBComparisonOperator op;

	public CockroachDBBinaryComparisonOperator(CockroachDBExpression left, CockroachDBExpression right,
			CockroachDBComparisonOperator op) {
		this.left = left;
		this.right = right;
		this.op = op;
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
		return op.textRepr;
	}

}
