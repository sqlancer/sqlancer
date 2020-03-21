package sqlancer.tdengine.expr;

import sqlancer.tdengine.TDEngineSchema.TDEngineColumn;

public class TDEngineColumnName extends TDEngineExpression {

	private TDEngineColumn c;
	private TDEngineConstant val;

	public TDEngineColumnName(TDEngineColumn c, TDEngineConstant val) {
		this.c = c;
		this.val = val;
	}
	
	public TDEngineColumn getColumn() {
		return c;
	}
	
	public TDEngineConstant getVal() {
		return val;
	}

	@Override
	public TDEngineConstant getExpectedValue() {
		return val;
	}

}
