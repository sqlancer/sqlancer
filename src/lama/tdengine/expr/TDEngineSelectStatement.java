package lama.tdengine.expr;

import java.util.List;

import lama.tdengine.TDEngineSchema.TDEngineTable;

public class TDEngineSelectStatement extends TDEngineExpression {

	private TDEngineTable table;
	private List<TDEngineExpression> columns;
	private TDEngineExpression whereClause;
	private TDEngineExpression limitClause;
	private TDEngineExpression offsetClause;
	private TDEngineExpression orderBy;

	public void setFromTable(TDEngineTable table) {
		this.table = table;
	}

	public void setFetchColumns(List<TDEngineExpression> colExpressions) {
		this.columns = colExpressions;
	}

	public void setWhereClause(TDEngineExpression whereClause) {
		this.whereClause = whereClause;
	}

	public void setLimitClause(TDEngineExpression limitClause) {
		this.limitClause = limitClause;
		
	}

	public void setOffsetClause(TDEngineExpression offsetClause) {
		this.offsetClause = offsetClause;
	}

	public void setOrderByClause(TDEngineExpression orderBy) {
		this.orderBy = orderBy;
	}

	@Override
	public TDEngineConstant getExpectedValue() {
		throw new AssertionError();
	}
	
	public List<TDEngineExpression> getColumns() {
		return columns;
	}
	public TDEngineExpression getLimitClause() {
		return limitClause;
	}
	
	public TDEngineExpression getOffsetClause() {
		return offsetClause;
	}
	
	public TDEngineExpression getOrderBy() {
		return orderBy;
	}
	
	public TDEngineTable getTable() {
		return table;
	}
	
	public TDEngineExpression getWhereClause() {
		return whereClause;
	}

}
