package sqlancer.tidb.ast;

import java.util.List;

public class TiDBCase implements TiDBExpression {

    private final List<TiDBExpression> conditions;
    private final List<TiDBExpression> expressions;
    private final TiDBExpression elseExpr;
    private final TiDBExpression switchCondition;

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
