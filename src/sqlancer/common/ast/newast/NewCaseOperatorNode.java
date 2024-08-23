package sqlancer.common.ast.newast;

import java.util.List;

public class NewCaseOperatorNode<T> {

    private final List<T> conditions;
    private final List<T> expressions;
    private final T elseExpr;
    private final T switchCondition;

    public NewCaseOperatorNode(T switchCondition, List<T> conditions, List<T> expressions, T elseExpr) {
        this.switchCondition = switchCondition;
        this.conditions = conditions;
        this.expressions = expressions;
        this.elseExpr = elseExpr;
        if (conditions.size() != expressions.size()) {
            throw new IllegalArgumentException();
        }
    }

    public T getSwitchCondition() {
        return switchCondition;
    }

    public List<T> getConditions() {
        return conditions;
    }

    public List<T> getExpressions() {
        return expressions;
    }

    public T getElseExpr() {
        return elseExpr;
    }

}
