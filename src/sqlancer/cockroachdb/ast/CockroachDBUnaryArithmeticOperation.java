package sqlancer.cockroachdb.ast;

import sqlancer.Randomly;
import sqlancer.ast.BinaryOperatorNode.Operator;
import sqlancer.ast.UnaryOperatorNode;
import sqlancer.cockroachdb.ast.CockroachDBUnaryArithmeticOperation.CockroachDBUnaryAritmeticOperator;

public class CockroachDBUnaryArithmeticOperation extends
		UnaryOperatorNode<CockroachDBExpression, CockroachDBUnaryAritmeticOperator> implements CockroachDBExpression {

	public enum CockroachDBUnaryAritmeticOperator implements Operator {
		PLUS("+"), MINUS("-"), NEGATION("~");

		private String textRepr;

		private CockroachDBUnaryAritmeticOperator(String textRepr) {
			this.textRepr = textRepr;
		}

		public static CockroachDBUnaryAritmeticOperator getRandom() {
			return Randomly.fromOptions(values());
		}

		@Override
		public String getTextRepresentation() {
			return textRepr;
		}

	}

	public CockroachDBUnaryArithmeticOperation(CockroachDBExpression expr, CockroachDBUnaryAritmeticOperator op) {
		super(expr, op);
	}

	@Override
	public OperatorKind getOperatorKind() {
		return OperatorKind.PREFIX;
	}

}
