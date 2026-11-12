package sqlancer.voltdb.ast;

import java.util.List;

/**
 * A searched CASE: CASE WHEN c1 THEN e1 [WHEN c2 THEN e2 ...] [ELSE e] END. Without ELSE, a row no condition holds for
 * gives NULL.
 */
public class VoltDBCase implements VoltDBExpression {

    private final List<VoltDBExpression> conditions;
    private final List<VoltDBExpression> results;
    private final VoltDBExpression elseResult;

    public VoltDBCase(List<VoltDBExpression> conditions, List<VoltDBExpression> results, VoltDBExpression elseResult) {
        if (conditions.isEmpty() || conditions.size() != results.size()) {
            throw new IllegalArgumentException();
        }
        this.conditions = List.copyOf(conditions);
        this.results = List.copyOf(results);
        this.elseResult = elseResult;
    }

    public List<VoltDBExpression> getConditions() {
        return conditions;
    }

    public List<VoltDBExpression> getResults() {
        return results;
    }

    // null when the CASE has no ELSE
    public VoltDBExpression getElseResult() {
        return elseResult;
    }

}
