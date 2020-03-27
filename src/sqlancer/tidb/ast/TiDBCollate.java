package sqlancer.tidb.ast;

import sqlancer.visitor.UnaryOperation;

public class TiDBCollate implements TiDBExpression, UnaryOperation<TiDBExpression>  {

	private final TiDBExpression expr;
	private final String collate;
	
	public TiDBCollate(TiDBExpression expr, String text) {
		this.expr = expr;
		this.collate = text;
	}

	@Override
	public TiDBExpression getExpression() {
		return expr;
	}

	@Override
	public String getOperatorRepresentation() {
		return String.format("COLLATE '%s'", collate);
	}

	@Override
	public OperatorKind getOperatorKind() {
		return OperatorKind.POSTFIX;
	}
	
}
