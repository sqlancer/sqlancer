package lama;

import java.util.List;

import lama.Expression.OrderingTerm;
import lama.schema.Schema.Table;

public class SelectStatement {

	private SelectType fromOptions;
	private List<Table> fromList;
	private Expression whereClause;
	private List<Expression> groupByClause;
	private Expression limitClause;
	private List<OrderingTerm> orderByClause;
	private Expression offsetClause;
	
	public enum SelectType {
		DISTINCT, ALL;
	}

	public void setSelectType(SelectType fromOptions) {
		this.setFromOptions(fromOptions);
	}


	public void setFromTables(List<Table> fromTables) {
		this.setFromList(fromTables);
	}

	public SelectType getFromOptions() {
		return fromOptions;
	}

	public void setFromOptions(SelectType fromOptions) {
		this.fromOptions = fromOptions;
	}

	public List<Table> getFromList() {
		return fromList;
	}

	public void setFromList(List<Table> fromList) {
		this.fromList = fromList;
	}


	public Expression getWhereClause() {
		return whereClause;
	}


	public void setWhereClause(Expression whereClause) {
		this.whereClause = whereClause;
	}


	public void setGroupByClause(List<Expression> groupByClause) {
		this.groupByClause = groupByClause;
	}
	
	public List<Expression> getGroupByClause() {
		return groupByClause;
	}


	public void setLimitClause(Expression limitClause) {
		this.limitClause = limitClause;
	}
	
	public Expression getLimitClause() {
		return limitClause;
	}


	public List<OrderingTerm> getOrderByClause() {
		return orderByClause;
	}


	public void setOrderByClause(List<OrderingTerm> orderBy) {
		this.orderByClause = orderBy;
	}


	public void setOffsetClause(Expression offsetClause) {
		this.offsetClause = offsetClause;
	}
	
	public Expression getOffsetClause() {
		return offsetClause;
	}


}
