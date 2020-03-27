package sqlancer.cockroachdb.ast;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class CockroachDBSelect implements CockroachDBExpression {
	
	private List<CockroachDBExpression> columns = new ArrayList<>();
	private List<CockroachDBTableReference> fromTables;
	private CockroachDBExpression whereCondition;
	private List<CockroachDBExpression> orderByTerms = new ArrayList<>();
	private boolean isDistinct;
	private CockroachDBExpression limit;
	private CockroachDBExpression offset;
	private CockroachDBExpression havingClause;
	private List<CockroachDBExpression> groupByExpression;
	private List<CockroachDBJoin> joinList = Collections.emptyList();

	
	public void setColumns(List<CockroachDBExpression> columns) {
		this.columns = columns;
	}
	
	public void setFromTables(List<CockroachDBTableReference> fromTables) {
		this.fromTables = fromTables;
	}
	
	public List<CockroachDBExpression> getColumns() {
		return columns;
	}
	
	public List<CockroachDBTableReference> getFromTables() {
		return fromTables;
	}

	public void setWhereCondition(CockroachDBExpression whereCondition) {
		this.whereCondition = whereCondition;
	}
	
	public CockroachDBExpression getWhereCondition() {
		return whereCondition;
	}
	
	public void setOrderByTerms(List<CockroachDBExpression> orderByTerms) {
		this.orderByTerms = orderByTerms;
	}
	
	public List<CockroachDBExpression> getOrderByTerms() {
		return orderByTerms;
	}


	public boolean isDistinct() {
		return isDistinct;
	}
	
	public void setDistinct(boolean isDistinct) {
		this.isDistinct = isDistinct;
	}

	public void setLimitTerm(CockroachDBExpression limit) {
		this.limit = limit;
	}

	public CockroachDBExpression getLimit() {
		return limit;
	}

	public void setOffset(CockroachDBExpression offset) {
		this.offset = offset;
	}
	
	public CockroachDBExpression getOffset() {
		return offset;
	}
	
	public void setHavingClause(CockroachDBExpression havingClause) {
		this.havingClause = havingClause;
	}
	
	public CockroachDBExpression getHavingClause() {
		return havingClause;
	}

	public void setGroupByClause(List<CockroachDBExpression> groupByExpression) {
		this.groupByExpression = groupByExpression;
	}

	public List<CockroachDBExpression> getGroupByExpression() {
		return groupByExpression;
	}

	public List<CockroachDBJoin> getJoinList() {
		return joinList;
	}
	
	public void setJoinList(List<CockroachDBJoin> joinList) {
		this.joinList = joinList;
	}
	
}
