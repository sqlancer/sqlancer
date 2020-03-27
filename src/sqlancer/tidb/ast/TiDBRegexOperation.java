package sqlancer.tidb.ast;

import sqlancer.Randomly;
import sqlancer.ast.BinaryNode;

public class TiDBRegexOperation extends BinaryNode<TiDBExpression> implements TiDBExpression {

	public enum TiDBRegexOperator {
		LIKE("LIKE"), //
		NOT_LIKE("NOT LIKE"), //
		ILIKE("REGEXP"), //
		NOT_REGEXP("NOT REGEXP");

		private String textRepr;

		private TiDBRegexOperator(String textRepr) {
			this.textRepr = textRepr;
		}

		public static TiDBRegexOperator getRandom() {
			return Randomly.fromOptions(values());
		}

	}

	private final TiDBRegexOperator op;

	public TiDBRegexOperation(TiDBExpression left, TiDBExpression right, TiDBRegexOperator op) {
		super(left, right);
		this.op = op;
	}

	@Override
	public String getOperatorRepresentation() {
		return op.textRepr;
	}

}
