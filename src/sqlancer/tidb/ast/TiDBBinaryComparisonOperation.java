package sqlancer.tidb.ast;

import sqlancer.Randomly;
import sqlancer.ast.BinaryNode;

public class TiDBBinaryComparisonOperation extends BinaryNode<TiDBExpression> implements TiDBExpression {

	public enum TiDBComparisonOperator {
		EQUALS("="), //
		GREATER(">"), //
		GREATER_EQUALS(">="), //
		SMALLER("<"), //
		SMALLER_EQUALS("<="), //
		NOT_EQUALS("!="), //
		NULL_SAFE_EQUALS("<=>");

		private String textRepr;

		private TiDBComparisonOperator(String textRepr) {
			this.textRepr = textRepr;
		}

		public static TiDBComparisonOperator getRandom() {
			return Randomly.fromOptions(TiDBComparisonOperator.values());
		}

	}

	private final TiDBComparisonOperator op;

	public TiDBBinaryComparisonOperation(TiDBExpression left, TiDBExpression right, TiDBComparisonOperator op) {
		super(left, right);
		this.op = op;
	}

	@Override
	public String getOperatorRepresentation() {
		return op.textRepr;
	}

}
