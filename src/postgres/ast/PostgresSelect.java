package postgres.ast;

import java.util.Collections;
import java.util.List;

import postgres.PostgresSchema.PostgresColumn;
import postgres.PostgresSchema.PostgresDataType;
import postgres.PostgresSchema.PostgresTable;

public class PostgresSelect extends PostgresExpression {

	private SelectType fromOptions;
	private List<PostgresTable> fromList = Collections.emptyList();
	private PostgresExpression whereClause;
	private List<PostgresExpression> groupByClause = Collections.emptyList();
	private PostgresExpression limitClause;
	private List<PostgresExpression> orderByClause = Collections.emptyList();
	private PostgresExpression offsetClause;
	private List<PostgresColumn> fetchColumns = Collections.emptyList();
	private List<String> modifiers = Collections.emptyList();

	public enum SelectType {
		DISTINCT, ALL;
	}

	public void setSelectType(SelectType fromOptions) {
		this.setFromOptions(fromOptions);
	}

	public void setFromTables(List<PostgresTable> fromTables) {
		this.setFromList(fromTables);
	}

	public SelectType getFromOptions() {
		return fromOptions;
	}

	public void setFromOptions(SelectType fromOptions) {
		this.fromOptions = fromOptions;
	}

	public List<PostgresTable> getFromList() {
		return fromList;
	}

	public void setFromList(List<PostgresTable> fromList) {
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

	public void selectFetchColumns(List<PostgresColumn> fetchColumns) {
		this.fetchColumns = fetchColumns;
	}

	public List<PostgresColumn> getFetchColumns() {
		return fetchColumns;
	}

	public void setModifiers(List<String> modifiers) {
		this.modifiers = modifiers;
	}

	public List<String> getModifiers() {
		return modifiers;
	}

	@Override
	public PostgresConstant getExpectedValue() {
		return null;
	}

	@Override
	public PostgresDataType getExpressionType() {
		return null;
	}
	
}
