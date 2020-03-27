package sqlancer.cockroachdb.ast;

import sqlancer.Randomly;
import sqlancer.ast.UnaryNode;

public class CockroachDBUnaryPostfixOperation extends UnaryNode<CockroachDBExpression> implements CockroachDBExpression {
	
	private final CockroachDBUnaryPostfixOperator op;
	
	public static enum CockroachDBUnaryPostfixOperator {
		IS_NULL("IS NULL"), //
		IS_NOT_NULL("IS NOT NULL"), //
		IS_NAN("IS NAN"), //
		IS_NOT_NAN("IS NOT NAN"),
		IS_TRUE("IS TRUE"),
		IS_FALSE("IS FALSE"),
		IS_NOT_TRUE("IS NOT TRUE"),
		IS_NOT_FALSE("IS NOT FALSE");
		
		private String s;
		
		private CockroachDBUnaryPostfixOperator(String s) {
			this.s = s;
		}
		
		public static CockroachDBUnaryPostfixOperator getRandom() {
			return Randomly.fromOptions(values());
		}
	}

	public CockroachDBUnaryPostfixOperation(CockroachDBExpression expr, CockroachDBUnaryPostfixOperator op) {
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
