package lama.sqlite3.ast;

import java.util.Collections;
import java.util.List;

import lama.sqlite3.schema.SQLite3Schema.Column;
import lama.sqlite3.schema.SQLite3Schema.Column.CollateSequence;
import lama.sqlite3.schema.SQLite3Schema.Table;

public class SQLite3SelectStatement extends SQLite3Expression {

	private SelectType fromOptions;
	private List<Table> fromList = Collections.emptyList();
	private SQLite3Expression whereClause;
	private List<SQLite3Expression> groupByClause = Collections.emptyList();
	private SQLite3Expression limitClause;
	private List<SQLite3Expression> orderByClause = Collections.emptyList();
	private SQLite3Expression offsetClause;
	private List<Column> fetchColumns = Collections.emptyList();
	private List<Join> joinStatements = Collections.emptyList();

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

	public SQLite3Expression getWhereClause() {
		return whereClause;
	}

	public void setWhereClause(SQLite3Expression whereClause) {
		this.whereClause = whereClause;
	}

	public void setGroupByClause(List<SQLite3Expression> groupByClause) {
		this.groupByClause = groupByClause;
	}

	public List<SQLite3Expression> getGroupByClause() {
		return groupByClause;
	}

	public void setLimitClause(SQLite3Expression limitClause) {
		this.limitClause = limitClause;
	}

	public SQLite3Expression getLimitClause() {
		return limitClause;
	}

	public List<SQLite3Expression> getOrderByClause() {
		return orderByClause;
	}

	public void setOrderByClause(List<SQLite3Expression> orderBy) {
		this.orderByClause = orderBy;
	}

	public void setOffsetClause(SQLite3Expression offsetClause) {
		this.offsetClause = offsetClause;
	}

	public SQLite3Expression getOffsetClause() {
		return offsetClause;
	}

	public void selectFetchColumns(List<Column> fetchColumns) {
		this.fetchColumns = fetchColumns;
	}

	public List<Column> getFetchColumns() {
		return fetchColumns;
	}

	public void setJoinClauses(List<Join> joinStatements) {
		this.joinStatements = joinStatements;
	}

	public List<Join> getJoinClauses() {
		return joinStatements;
	}

	@Override
	public CollateSequence getExplicitCollateSequence() {
		// TODO implement?
		return null;
	}

}
