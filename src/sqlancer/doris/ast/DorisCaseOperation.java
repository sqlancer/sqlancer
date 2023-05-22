package sqlancer.doris.ast;

import java.util.List;

import sqlancer.common.ast.newast.Node;

public class DorisCaseOperation implements Node<DorisExpression>, DorisExpression {

    private final DorisExpression expr;
    private final List<DorisExpression> conditions;
    private final List<DorisExpression> thenClauses;
    private final DorisExpression elseClause;

    public DorisCaseOperation(DorisExpression expr, List<DorisExpression> conditions, List<DorisExpression> thenClauses,
            DorisExpression elseClause) {
        this.expr = expr;
        this.conditions = conditions;
        this.thenClauses = thenClauses;
        this.elseClause = elseClause;
    }

    public DorisExpression getExpr() {
        return expr;
    }

    public List<DorisExpression> getConditions() {
        return conditions;
    }

    public List<DorisExpression> getThenClauses() {
        return thenClauses;
    }

    public DorisExpression getElseClause() {
        return elseClause;
    }

}
