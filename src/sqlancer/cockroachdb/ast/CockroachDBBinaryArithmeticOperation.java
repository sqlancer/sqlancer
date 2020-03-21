package sqlancer.cockroachdb.ast;

import sqlancer.Randomly;
import sqlancer.visitor.BinaryOperation;

public class CockroachDBBinaryArithmeticOperation extends CockroachDBExpression implements BinaryOperation<CockroachDBExpression> {

	private final CockroachDBExpression left;
	private final CockroachDBExpression right;
	private final CockroachDBBinaryArithmeticOperator op;

	public static enum CockroachDBBinaryArithmeticOperator {
		ADD("+"), MULT("*"), MINUS("-"), DIV("/");
		
		String textRepresentation;
		
		CockroachDBBinaryArithmeticOperator(String textRepresentation) {
			this.textRepresentation = textRepresentation;
		}
		
		public static CockroachDBBinaryArithmeticOperator getRandom() {
			return Randomly.fromOptions(values());
		}
	}
	
	public CockroachDBBinaryArithmeticOperation(CockroachDBExpression left, CockroachDBExpression right, CockroachDBBinaryArithmeticOperator op) {
		this.left = left;
		this.right = right;
		this.op = op;
	}
	
	@Override
	public CockroachDBExpression getLeft() {
		return left;
	}

	@Override
	public CockroachDBExpression getRight() {
		return right;
	}

	@Override
	public String getOperatorRepresentation() {
		return op.textRepresentation;
	}

}
