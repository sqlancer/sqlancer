package sqlancer.tidb.ast;

import sqlancer.Randomly;
import sqlancer.visitor.UnaryOperation;

public class TiDBUnaryPrefixOperation implements TiDBExpression, UnaryOperation<TiDBExpression>   {

	private final TiDBExpression expr;
	private final TiDBUnaryPrefixOperator op;
	
	public static enum TiDBUnaryPrefixOperator {
		NOT("NOT"); //
		
		private String s;
		
		private TiDBUnaryPrefixOperator(String s) {
			this.s = s;
		}
		
		public static TiDBUnaryPrefixOperator getRandom() {
			return Randomly.fromOptions(values());
		}
	}

	public TiDBUnaryPrefixOperation(TiDBExpression expr, TiDBUnaryPrefixOperator op) {
		this.expr = expr;
		this.op = op;
	}

	@Override
	public TiDBExpression getExpression() {
		return expr;
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