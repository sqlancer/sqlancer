package lama.cockroachdb.ast;

import lama.Randomly;
import lama.visitor.UnaryOperation;

public class CockroachDBUnaryPostfixOperation extends CockroachDBExpression implements UnaryOperation<CockroachDBExpression> {
	
	private final CockroachDBExpression expr;
	private final CockroachDBUnaryPostfixOperator op;
	
	public static enum CockroachDBUnaryPostfixOperator {
		IS_NULL("IS NULL"), //
		IS_NOT_NULL("IS NOT NULL"), //
		IS_NAN("IS NAN"), //
		IS_NOT_NAN("IS NOT NAN");
		
		private String s;
		
		private CockroachDBUnaryPostfixOperator(String s) {
			this.s = s;
		}
		
		public static CockroachDBUnaryPostfixOperator getRandom() {
			return Randomly.fromOptions(values());
		}
	}

	public CockroachDBUnaryPostfixOperation(CockroachDBExpression expr, CockroachDBUnaryPostfixOperator op) {
		this.expr = expr;
		this.op = op;
	}

	@Override
	public CockroachDBExpression getExpression() {
		return expr;
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
