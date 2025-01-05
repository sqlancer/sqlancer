package sqlancer.duckdb.ast;

import java.util.LinkedHashMap;
import java.util.List;

public class DuckDBResultMap implements DuckDBExpression {
    private final LinkedHashMap<DuckDBColumnReference, List<DuckDBExpression>> DBStates;
    private final List<DuckDBExpression> results;

    public DuckDBResultMap(LinkedHashMap<DuckDBColumnReference, List<DuckDBExpression>> s, List<DuckDBExpression> r) {
        this.DBStates = s;
        this.results = r;
        if (s.get(s.keySet().iterator().next()).size() != r.size()) {
            throw new AssertionError();
        }
    }

    public LinkedHashMap<DuckDBColumnReference, List<DuckDBExpression>> getDbStates() {
        return this.DBStates;
    }

    public List<DuckDBExpression> getResult() {
        return this.results;
    }
}
