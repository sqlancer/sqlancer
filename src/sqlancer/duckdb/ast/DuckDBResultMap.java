package sqlancer.duckdb.ast;

import java.util.List;
import java.util.Map;

public class DuckDBResultMap implements DuckDBExpression {
    private final Map<DuckDBColumnReference, List<DuckDBExpression>> dbStates;
    private final List<DuckDBExpression> results;

    public DuckDBResultMap(Map<DuckDBColumnReference, List<DuckDBExpression>> s, List<DuckDBExpression> r) {
        this.dbStates = s;
        this.results = r;
        if (s.get(s.keySet().iterator().next()).size() != r.size()) {
            throw new AssertionError();
        }
    }

    public Map<DuckDBColumnReference, List<DuckDBExpression>> getDbStates() {
        return this.dbStates;
    }

    public List<DuckDBExpression> getResult() {
        return this.results;
    }
}
