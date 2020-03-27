package sqlancer.tidb.ast;

import sqlancer.Randomly;
import sqlancer.visitor.BinaryOperation;

public class TiDBBinaryComparisonOperation extends TiDBExpression
implements BinaryOperation<TiDBExpression> {


	public enum TiDBComparisonOperator {
		EQUALS("="), //
		GREATER(">"), //
		GREATER_EQUALS(">="), //
		SMALLER("<"), //
		SMALLER_EQUALS("<="), //
		NOT_EQUALS("!="),
		NULL_SAFE_EQUALS("<=>");

		private String textRepr;

		private TiDBComparisonOperator(String textRepr) {
			this.textRepr = textRepr;
		}

		public static TiDBComparisonOperator getRandom() {
			return Randomly.fromOptions(TiDBComparisonOperator.values());
		}

	}

	private final TiDBExpression left;
	private final TiDBExpression right;
	private final TiDBComparisonOperator op;

	public TiDBBinaryComparisonOperation(TiDBExpression left, TiDBExpression right,
			TiDBComparisonOperator op) {
		this.left = left;
		this.right = right;
		this.op = op;
	}

	@Override
	public TiDBExpression getLeft() {
		return left;
	}

	@Override
	public TiDBExpression getRight() {
		return right;
	}

	@Override
	public String getOperatorRepresentation() {
		return op.textRepr;
	}

}
