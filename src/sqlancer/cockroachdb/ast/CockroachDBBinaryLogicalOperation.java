package sqlancer.cockroachdb.ast;

import sqlancer.Randomly;
import sqlancer.ast.BinaryNode;

public class CockroachDBBinaryLogicalOperation extends BinaryNode<CockroachDBExpression> implements CockroachDBExpression  {
	
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
	
	private final CockroachDBBinaryLogicalOperator op;

	public CockroachDBBinaryLogicalOperation(CockroachDBExpression left, CockroachDBExpression right, CockroachDBBinaryLogicalOperator op) {
		super(left, right);
		this.op = op;
	}

	@Override
	public String getOperatorRepresentation() {
		return op.textRepr;
	}

}
