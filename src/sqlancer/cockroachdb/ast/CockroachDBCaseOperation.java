package sqlancer.cockroachdb.ast;

import java.util.List;

public class CockroachDBCaseOperation implements CockroachDBExpression {

    private final List<CockroachDBExpression> conditions;
    private final List<CockroachDBExpression> thenClauses;
    private final CockroachDBExpression elseClause;

    public CockroachDBCaseOperation(List<CockroachDBExpression> conditions, List<CockroachDBExpression> thenClauses,
            CockroachDBExpression elseClause) {
        this.conditions = conditions;
        this.thenClauses = thenClauses;
        this.elseClause = elseClause;
    }

    public List<CockroachDBExpression> getConditions() {
        return conditions;
    }

    public List<CockroachDBExpression> getThenClauses() {
        return thenClauses;
    }

    public CockroachDBExpression getElseClause() {
        return elseClause;
    }

}
