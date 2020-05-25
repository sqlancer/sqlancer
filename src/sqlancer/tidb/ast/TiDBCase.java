package sqlancer.tidb.ast;

import java.util.List;

public class TiDBCase implements TiDBExpression {

	private List<TiDBExpression> conditions;
	private List<TiDBExpression> expressions;
	private TiDBExpression elseExpr;
	private TiDBExpression switchCondition;

	public TiDBCase(TiDBExpression switchCondition, List<TiDBExpression> conditions, List<TiDBExpression> expressions,
			TiDBExpression elseExpr) {
		this.switchCondition = switchCondition;
		this.conditions = conditions;
		this.expressions = expressions;
		this.elseExpr = elseExpr;
		if (conditions.size() != expressions.size()) {
			throw new IllegalArgumentException();
		}
	}

	public TiDBExpression getSwitchCondition() {
		return switchCondition;
	}

	public List<TiDBExpression> getConditions() {
		return conditions;
	}

	public List<TiDBExpression> getExpressions() {
		return expressions;
	}

	public TiDBExpression getElseExpr() {
		return elseExpr;
	}

}
