package sqlancer.cockroachdb.ast;

import sqlancer.Randomly;
import sqlancer.ast.BinaryOperatorNode;
import sqlancer.ast.BinaryOperatorNode.Operator;
import sqlancer.cockroachdb.ast.CockroachDBBinaryArithmeticOperation.CockroachDBBinaryArithmeticOperator;

public class CockroachDBBinaryArithmeticOperation
		extends BinaryOperatorNode<CockroachDBExpression, CockroachDBBinaryArithmeticOperator>
		implements CockroachDBExpression {

	public static enum CockroachDBBinaryArithmeticOperator implements Operator {
		ADD("+"), MULT("*"), MINUS("-"), DIV("/");

		String textRepresentation;

		CockroachDBBinaryArithmeticOperator(String textRepresentation) {
			this.textRepresentation = textRepresentation;
		}

		public static CockroachDBBinaryArithmeticOperator getRandom() {
			return Randomly.fromOptions(values());
		}

		@Override
		public String getTextRepresentation() {
			return textRepresentation;
		}

	}

	public CockroachDBBinaryArithmeticOperation(CockroachDBExpression left, CockroachDBExpression right,
			CockroachDBBinaryArithmeticOperator op) {
		super(left, right, op);
	}

}
