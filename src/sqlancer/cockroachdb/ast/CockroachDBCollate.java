package sqlancer.cockroachdb.ast;

import sqlancer.visitor.UnaryOperation;

public class CockroachDBCollate implements UnaryOperation<CockroachDBExpression>, CockroachDBExpression {

	private final CockroachDBExpression expr;
	private final String collate;

	public CockroachDBCollate(CockroachDBExpression expr, String collate) {
		this.expr = expr;
		this.collate = collate;
	}
	
	public String getCollate() {
		return collate;
	}

	@Override
	public CockroachDBExpression getExpression() {
		return expr;
	}

	@Override
	public String getOperatorRepresentation() {
		return "COLLATE " + collate;
	}

	@Override
	public OperatorKind getOperatorKind() {
		return OperatorKind.POSTFIX;
	}
	
}
