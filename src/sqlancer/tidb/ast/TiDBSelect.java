package sqlancer.tidb.ast;

import java.util.ArrayList;
import java.util.List;

public class TiDBSelect implements TiDBExpression {

	private TiDBExpression wherePredicate;
	private List<TiDBExpression> fetchColumns;
	private List<TiDBExpression> from;
	private List<TiDBExpression> orderBy = new ArrayList<>();
	private List<TiDBExpression> groupBys = new ArrayList<>();
	private TiDBExpression havingClause;

	public void setWhereCondition(TiDBExpression wherePredicate) {
		this.wherePredicate = wherePredicate;
	}

	public TiDBExpression getWherePredicate() {
		return wherePredicate;
	}

	public List<TiDBExpression> getFetchColumns() {
		return fetchColumns;
	}
	
	public void setColumns(List<TiDBExpression> fetchColumns) {
		this.fetchColumns = fetchColumns;
	}

	public void setFromTables(List<TiDBExpression> from) {
		this.from = from;
	}

	public List<TiDBExpression> getFrom() {
		return from;
	}

	public void setOrderBy(List<TiDBExpression> orderBy) {
		this.orderBy = orderBy;
	}

	public List<TiDBExpression> getOrderBy() {
		return orderBy;
	}

	public void setGroupByClause(List<TiDBExpression> groupBys) {
		this.groupBys = groupBys;
	}

	public List<TiDBExpression> getGroupBys() {
		return groupBys;
	}

	public void setHavingClause(TiDBExpression havingClause) {
		this.havingClause = havingClause;
	}
	
	public TiDBExpression getHavingClause() {
		return havingClause;
	}
	
}
