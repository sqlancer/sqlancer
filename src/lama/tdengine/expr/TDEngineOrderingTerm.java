package lama.tdengine.expr;

public class TDEngineOrderingTerm extends TDEngineExpression {

	private TDEngineExpression expr;
	private TDOrdering order;

	public TDEngineOrderingTerm(TDEngineExpression expr, TDOrdering order) {
		this.expr = expr;
		this.order = order;
	}

	public enum TDOrdering {
		ASC, DESC;
	}

	@Override
	public TDEngineConstant getExpectedValue() {
		return expr.getExpectedValue();
	}
	
	public TDEngineExpression getExpr() {
		return expr;
	}
	
	public TDOrdering getOrder() {
		return order;
	}

}
