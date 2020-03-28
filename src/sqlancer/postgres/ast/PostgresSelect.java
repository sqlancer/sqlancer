package sqlancer.postgres.ast;

import java.util.Collections;
import java.util.List;

import sqlancer.Randomly;
import sqlancer.postgres.PostgresSchema.PostgresDataType;
import sqlancer.postgres.PostgresSchema.PostgresTable;

public class PostgresSelect implements PostgresExpression {
	
	public static class PostgresFromTable {
		private PostgresTable t;
		private boolean only;

		public PostgresFromTable(PostgresTable t, boolean only) {
			this.t = t;
			this.only = only;
		}
		
		public PostgresTable getTable() {
			return t;
		}
		
		public boolean isOnly() {
			return only;
		}
	}

	private SelectType selectOption;
	private List<PostgresFromTable> fromList = Collections.emptyList();
	private PostgresExpression whereClause;
	private List<PostgresExpression> groupByClause = Collections.emptyList();
	private PostgresExpression limitClause;
	private List<PostgresExpression> orderByClause = Collections.emptyList();
	private PostgresExpression offsetClause;
	private List<PostgresExpression> fetchColumns = Collections.emptyList();
	private List<PostgresJoin> joinClauses = Collections.emptyList();
	private PostgresExpression distinctOnClause;

	public enum SelectType {
		DISTINCT, ALL;

		public static SelectType getRandom() {
			return Randomly.fromOptions(values());
		}
	}

	public void setSelectType(SelectType fromOptions) {
		this.setSelectOption(fromOptions);
	}
	
	public void setDistinctOnClause(PostgresExpression distinctOnClause) {
		if (selectOption != SelectType.DISTINCT) {
			throw new IllegalArgumentException();
		}
		this.distinctOnClause = distinctOnClause;
	}

	public void setFromTables(List<PostgresFromTable> fromTables) {
		this.setFromList(fromTables);
	}

	public SelectType getSelectOption() {
		return selectOption;
	}

	public void setSelectOption(SelectType fromOptions) {
		this.selectOption = fromOptions;
	}

	public List<PostgresFromTable> getFromList() {
		return fromList;
	}

	public void setFromList(List<PostgresFromTable> fromList) {
		this.fromList = fromList;
	}

	public PostgresExpression getWhereClause() {
		return whereClause;
	}

	public void setWhereClause(PostgresExpression whereClause) {
		this.whereClause = whereClause;
	}

	public void setGroupByClause(List<PostgresExpression> groupByClause) {
		this.groupByClause = groupByClause;
	}

	public List<PostgresExpression> getGroupByClause() {
		return groupByClause;
	}

	public void setLimitClause(PostgresExpression limitClause) {
		this.limitClause = limitClause;
	}

	public PostgresExpression getLimitClause() {
		return limitClause;
	}

	public List<PostgresExpression> getOrderByClause() {
		return orderByClause;
	}

	public void setOrderByClause(List<PostgresExpression> orderBy) {
		this.orderByClause = orderBy;
	}

	public void setOffsetClause(PostgresExpression offsetClause) {
		this.offsetClause = offsetClause;
	}

	public PostgresExpression getOffsetClause() {
		return offsetClause;
	}

	public void setFetchColumns(List<PostgresExpression> fetchColumns) {
		this.fetchColumns = fetchColumns;
	}

	public List<PostgresExpression> getFetchColumns() {
		return fetchColumns;
	}

	@Override
	public PostgresConstant getExpectedValue() {
		return null;
	}

	@Override
	public PostgresDataType getExpressionType() {
		return null;
	}

	public void setJoinClauses(List<PostgresJoin> joinStatements) {
		this.joinClauses = joinStatements;
		
	}

	public List<PostgresJoin> getJoinClauses() {
		return joinClauses;
	}
	
	public PostgresExpression getDistinctOnClause() {
		return distinctOnClause;
	}
	
	
}
