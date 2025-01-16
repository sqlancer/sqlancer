package sqlancer.tidb.ast;

import java.util.LinkedHashMap;
import java.util.List;

import sqlancer.tidb.TiDBSchema.TiDBCompositeDataType;

public class TiDBResultMap implements TiDBExpression {
    private final LinkedHashMap<TiDBColumnReference, List<TiDBConstant>> DBStates;
    private final List<TiDBConstant> results;
    TiDBCompositeDataType resultType;

    public TiDBResultMap(LinkedHashMap<TiDBColumnReference, List<TiDBConstant>> s, List<TiDBConstant> r, TiDBCompositeDataType rt) {
        this.DBStates = s;
        this.results = r;
        this.resultType = rt;
        if (s.get(s.keySet().iterator().next()).size() != r.size()) {
            throw new AssertionError();
        }
    }

    public LinkedHashMap<TiDBColumnReference, List<TiDBConstant>> getDbStates() {
        return this.DBStates;
    }

    public List<TiDBConstant> getResult() {
        return this.results;
    }

    public TiDBCompositeDataType getResultType() {
        return this.resultType;
    }
}
