package sqlancer.tidb.ast;

import sqlancer.Randomly;
import sqlancer.ast.UnaryNode;

public class TiDBUnaryPrefixOperation extends UnaryNode<TiDBExpression> implements TiDBExpression {

	private final TiDBUnaryPrefixOperator op;

	public static enum TiDBUnaryPrefixOperator {
		NOT("NOT"), //
		INVERSION("~"), //
		PLUS("+"), //
		MINUS("-"), //
		BINARY("BINARY"); //

		private String s;

		private TiDBUnaryPrefixOperator(String s) {
			this.s = s;
		}

		public static TiDBUnaryPrefixOperator getRandom() {
			return Randomly.fromOptions(values());
		}
	}

	public TiDBUnaryPrefixOperation(TiDBExpression expr, TiDBUnaryPrefixOperator op) {
		super(expr);
		this.op = op;
	}

	@Override
	public String getOperatorRepresentation() {
		return op.s;
	}

	@Override
	public OperatorKind getOperatorKind() {
		return OperatorKind.PREFIX;
	}

}