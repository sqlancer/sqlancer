package lama.cockroachdb.ast;

import java.util.ArrayList;
import java.util.List;

import lama.cockroachdb.CockroachDBSchema.CockroachDBTable;

public class CockroachDBSelect extends CockroachDBExpression {
	
	private List<CockroachDBExpression> columns = new ArrayList<>();
	private List<CockroachDBTable> fromTables;
	private CockroachDBExpression whereCondition;
	private List<CockroachDBExpression> orderByTerms = new ArrayList<>();
	private boolean isDistinct;
	private CockroachDBExpression limit;
	private CockroachDBExpression offset;
	
	public void setColumns(List<CockroachDBExpression> columns) {
		this.columns = columns;
	}
	
	public void setFromTables(List<CockroachDBTable> fromTables) {
		this.fromTables = fromTables;
	}
	
	public List<CockroachDBExpression> getColumns() {
		return columns;
	}
	
	public List<CockroachDBTable> getFromTables() {
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
	
}
