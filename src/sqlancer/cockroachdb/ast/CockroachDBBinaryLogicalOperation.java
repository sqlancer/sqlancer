package sqlancer.cockroachdb.ast;

import sqlancer.Randomly;
import sqlancer.visitor.BinaryOperation;

public class CockroachDBBinaryLogicalOperation implements BinaryOperation<CockroachDBExpression>, CockroachDBExpression  {
	
	public enum CockroachDBBinaryLogicalOperator {
		AND("AND"), OR("OR");
		
		private String textRepr;

		private CockroachDBBinaryLogicalOperator(String textRepr) {
			this.textRepr = textRepr;
		}

		public static CockroachDBBinaryLogicalOperator getRandom() {
			return Randomly.fromOptions(CockroachDBBinaryLogicalOperator.values());
		}
		
	}
	
	private final CockroachDBExpression left;
	private final CockroachDBExpression right;
	private final CockroachDBBinaryLogicalOperator op;

	public CockroachDBBinaryLogicalOperation(CockroachDBExpression left, CockroachDBExpression right, CockroachDBBinaryLogicalOperator op) {
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
		return op.textRepr;
	}

}
