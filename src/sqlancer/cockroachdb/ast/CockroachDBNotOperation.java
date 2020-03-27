package sqlancer.cockroachdb.ast;

import sqlancer.visitor.UnaryOperation;

public class CockroachDBNotOperation implements UnaryOperation<CockroachDBExpression>, CockroachDBExpression {
	
	private CockroachDBExpression expr;

	public CockroachDBNotOperation(CockroachDBExpression expr) {
		this.expr = expr;
	}

	@Override
	public CockroachDBExpression getExpression() {
		return expr;
	}

	@Override
	public String getOperatorRepresentation() {
		return "NOT";
	}

	@Override
	public OperatorKind getOperatorKind() {
		return OperatorKind.PREFIX;
	}

}
