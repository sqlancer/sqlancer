package lama.cockroachdb.ast;

import lama.visitor.UnaryOperation;

public class CockroachDBAlias extends CockroachDBExpression implements UnaryOperation<CockroachDBExpression> {

	private CockroachDBExpression expr;
	private String alias;

	public CockroachDBAlias(CockroachDBExpression expr, String alias) {
		this.expr = expr;
		this.alias = alias;
	}
	
	@Override
	public CockroachDBExpression getExpression() {
		return expr;
	}

	@Override
	public String getOperatorRepresentation() {
		return " as " + alias;
	}

	@Override
	public OperatorKind getOperatorKind() {
		return OperatorKind.POSTFIX;
	}
	
	@Override
	public boolean omitBracketsWhenPrinting() {
		return true;
	}

}
