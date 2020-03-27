package sqlancer.tidb.ast;

import sqlancer.Randomly;
import sqlancer.visitor.BinaryOperation;

public class TiDBRegexOperation extends TiDBExpression implements BinaryOperation<TiDBExpression> {

	public enum TiDBRegexOperator {
		LIKE("LIKE"), //
		NOT_LIKE("NOT LIKE"), //
		ILIKE("REGEXP"),
		NOT_REGEXP("NOT REGEXP");

		private String textRepr;

		private TiDBRegexOperator(String textRepr) {
			this.textRepr = textRepr;
		}

		public static TiDBRegexOperator getRandom() {
			return Randomly.fromOptions(values());
		}

	}

	private final TiDBExpression left;
	private final TiDBExpression right;
	private final TiDBRegexOperator op;

	public TiDBRegexOperation(TiDBExpression left, TiDBExpression right,
				TiDBRegexOperator op) {
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
