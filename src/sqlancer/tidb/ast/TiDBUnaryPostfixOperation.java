package sqlancer.tidb.ast;

import sqlancer.Randomly;
import sqlancer.ast.UnaryNode;

public class TiDBUnaryPostfixOperation extends UnaryNode<TiDBExpression>  implements TiDBExpression  {

	private final TiDBUnaryPostfixOperator op;
	
	public static enum TiDBUnaryPostfixOperator {
		IS_NULL("IS NULL"), //
		IS_NOT_NULL("IS NOT NULL"); //
		
		private String s;
		
		private TiDBUnaryPostfixOperator(String s) {
			this.s = s;
		}
		
		public static TiDBUnaryPostfixOperator getRandom() {
			return Randomly.fromOptions(values());
		}
	}

	public TiDBUnaryPostfixOperation(TiDBExpression expr, TiDBUnaryPostfixOperator op) {
		super(expr);
		this.op = op;
	}

	@Override
	public String getOperatorRepresentation() {
		return op.s;
	}

	@Override
	public OperatorKind getOperatorKind() {
		return OperatorKind.POSTFIX;
	}

}