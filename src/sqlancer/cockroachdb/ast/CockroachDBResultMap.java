package sqlancer.cockroachdb.ast;

import java.util.LinkedHashMap;
import java.util.List;

import sqlancer.cockroachdb.CockroachDBSchema.CockroachDBCompositeDataType;

public class CockroachDBResultMap implements CockroachDBExpression {
    private final LinkedHashMap<CockroachDBColumnReference, List<CockroachDBConstant>> DBStates;
    private final List<CockroachDBConstant> results;
    CockroachDBCompositeDataType resultType;

    public CockroachDBResultMap(LinkedHashMap<CockroachDBColumnReference, List<CockroachDBConstant>> s, List<CockroachDBConstant> r, CockroachDBCompositeDataType rt) {
        this.DBStates = s;
        this.results = r;
        this.resultType = rt;
        if (s.get(s.keySet().iterator().next()).size() != r.size()) {
            throw new AssertionError();
        }
    }

    public LinkedHashMap<CockroachDBColumnReference, List<CockroachDBConstant>> getDbStates() {
        return this.DBStates;
    }

    public List<CockroachDBConstant> getResult() {
        return this.results;
    }

    public CockroachDBCompositeDataType getResultType() {
        return this.resultType;
    }
}
