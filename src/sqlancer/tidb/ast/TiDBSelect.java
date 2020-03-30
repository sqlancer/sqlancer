package sqlancer.tidb.ast;

import java.util.Collections;
import java.util.List;

import sqlancer.ast.SelectBase;

public class TiDBSelect extends SelectBase<TiDBExpression> implements TiDBExpression {

	private List<TiDBExpression> joinExpressions = Collections.emptyList();

	public void setJoins(List<TiDBExpression> joinExpressions) {
		this.joinExpressions = joinExpressions;
	}

	public List<TiDBExpression> getJoinExpressions() {
		return joinExpressions;
	}

}
