package sqlancer.tidb.ast;

import sqlancer.Randomly;
import sqlancer.ast.BinaryNode;

public class TiDBBinaryLogicalOperation extends BinaryNode<TiDBExpression> implements TiDBExpression {

	private final TiDBBinaryLogicalOperator op;

	public static enum TiDBBinaryLogicalOperator {
		AND("&"), //
		OR("|"), //
		XOR("^"), //
		LEFT_SHIFT("<<"), //
		RIGHT_SHIFT(">>");

		String textRepresentation;

		TiDBBinaryLogicalOperator(String textRepresentation) {
			this.textRepresentation = textRepresentation;
		}

		public static TiDBBinaryLogicalOperator getRandom() {
			return Randomly.fromOptions(values());
		}
	}

	public TiDBBinaryLogicalOperation(TiDBExpression left, TiDBExpression right, TiDBBinaryLogicalOperator op) {
		super(left, right);
		this.op = op;
	}

	@Override
	public String getOperatorRepresentation() {
		return op.textRepresentation;
	}

}
