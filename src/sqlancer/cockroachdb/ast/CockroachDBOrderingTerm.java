package sqlancer.cockroachdb.ast;

import sqlancer.visitor.UnaryOperation;

public class CockroachDBOrderingTerm extends CockroachDBExpression implements UnaryOperation<CockroachDBExpression> {
	
	private final CockroachDBExpression expr;
	private final boolean asc;

	public CockroachDBOrderingTerm(CockroachDBExpression expr, boolean asc) {
		this.expr = expr;
		this.asc = asc;
	}

	@Override
	public CockroachDBExpression getExpression() {
		return expr;
	}

	@Override
	public String getOperatorRepresentation() {
		return asc ? "ASC" : "DESC";
	}

	@Override
	public OperatorKind getOperatorKind() {
		return OperatorKind.POSTFIX;
	}

}
