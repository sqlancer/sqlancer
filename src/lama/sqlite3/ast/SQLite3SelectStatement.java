package lama.sqlite3.ast;

import java.util.Collections;
import java.util.List;

import lama.sqlite3.schema.SQLite3Schema.SQLite3Column.CollateSequence;

public class SQLite3SelectStatement extends SQLite3Expression {

	private SelectType fromOptions = SelectType.ALL;
	private List<SQLite3Expression> fromList = Collections.emptyList();
	private SQLite3Expression whereClause;
	private List<SQLite3Expression> groupByClause = Collections.emptyList();
	private SQLite3Expression limitClause;
	private List<SQLite3Expression> orderByClause = Collections.emptyList();
	private SQLite3Expression offsetClause;
	private List<SQLite3Expression> fetchColumns = Collections.emptyList();
	private List<Join> joinStatements = Collections.emptyList();
	private SQLite3Expression havingClause;

	public enum SelectType {
		DISTINCT, ALL;
	}

	public void setSelectType(SelectType fromOptions) {
		this.setFromOptions(fromOptions);
	}

	public void setFromTables(List<SQLite3Expression> fromTables) {
		this.setFromList(fromTables);
	}

	public SelectType getFromOptions() {
		return fromOptions;
	}

	public void setFromOptions(SelectType fromOptions) {
		this.fromOptions = fromOptions;
	}

	public List<SQLite3Expression> getFromList() {
		return fromList;
	}

	public void setFromList(List<SQLite3Expression> fromList) {
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

	public void setFetchColumns(List<SQLite3Expression> fetchColumns) {
		this.fetchColumns = fetchColumns;
	}

	public List<SQLite3Expression> getFetchColumns() {
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

	public void setHavingClause(SQLite3Expression havingClause) {
		this.havingClause = havingClause;
	}
	
	public SQLite3Expression getHavingClause() {
		assert orderByClause != null;
		return havingClause;
	}

}
