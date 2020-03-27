package sqlancer.tidb.ast;

import sqlancer.Randomly;
import sqlancer.ast.BinaryNode;

public class TiDBBinaryArithmeticOperation extends BinaryNode<TiDBExpression> implements TiDBExpression {

	private final TiDBBinaryArithmeticOperator op;

	public static enum TiDBBinaryArithmeticOperator {
		ADD("+"), // 
		MINUS("-"), //
		MULT("*"), //
		DIV("/"), //
		INTEGER_DIV("DIV"), //
		MOD("%"); //

		String textRepresentation;

		TiDBBinaryArithmeticOperator(String textRepresentation) {
			this.textRepresentation = textRepresentation;
		}

		public static TiDBBinaryArithmeticOperator getRandom() {
			return Randomly.fromOptions(values());
		}
	}

	public TiDBBinaryArithmeticOperation(TiDBExpression left, TiDBExpression right, TiDBBinaryArithmeticOperator op) {
		super(left, right);
		this.op = op;
	}

	@Override
	public String getOperatorRepresentation() {
		return op.textRepresentation;
	}

}
