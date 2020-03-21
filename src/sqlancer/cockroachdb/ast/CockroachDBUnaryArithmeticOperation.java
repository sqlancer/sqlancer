package sqlancer.cockroachdb.ast;

import sqlancer.Randomly;
import sqlancer.visitor.UnaryOperation;

public class CockroachDBUnaryArithmeticOperation extends CockroachDBExpression implements UnaryOperation<CockroachDBExpression> {
	
	private final CockroachDBExpression expr;
	private final CockroachDBUnaryAritmeticOperator op;

	public enum CockroachDBUnaryAritmeticOperator {
		PLUS("+"), MINUS("-"), NEGATION("~");
		
		private String textRepr;

		private CockroachDBUnaryAritmeticOperator(String textRepr) {
			this.textRepr = textRepr;
		}

		public static CockroachDBUnaryAritmeticOperator getRandom() {
			return Randomly.fromOptions(values());
		}
	
	}
	
	public CockroachDBUnaryArithmeticOperation(CockroachDBExpression expr, CockroachDBUnaryAritmeticOperator op) {
		this.expr = expr;
		this.op = op;
	}

	@Override
	public CockroachDBExpression getExpression() {
		return expr;
	}

	@Override
	public String getOperatorRepresentation() {
		return op.textRepr;
	}

	@Override
	public OperatorKind getOperatorKind() {
		return OperatorKind.PREFIX;
	}

}
