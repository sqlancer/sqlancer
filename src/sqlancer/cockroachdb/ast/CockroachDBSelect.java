package sqlancer.cockroachdb.ast;

import java.util.Collections;
import java.util.List;

import sqlancer.ast.SelectBase;

public class CockroachDBSelect extends SelectBase<CockroachDBExpression> implements CockroachDBExpression {
	
	private boolean isDistinct;
	private List<CockroachDBJoin> joinList = Collections.emptyList();

	
	public boolean isDistinct() {
		return isDistinct;
	}
	
	public void setDistinct(boolean isDistinct) {
		this.isDistinct = isDistinct;
	}

	public List<CockroachDBJoin> getJoinList() {
		return joinList;
	}
	
	public void setJoinList(List<CockroachDBJoin> joinList) {
		this.joinList = joinList;
	}
	
}
