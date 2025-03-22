package sqlancer.cockroachdb.ast;

import java.util.LinkedHashMap;
import java.util.List;

import sqlancer.cockroachdb.CockroachDBSchema.CockroachDBColumn;

public class CockroachDBValues implements CockroachDBExpression {

    private final LinkedHashMap<CockroachDBColumn, List<CockroachDBConstant>> values;

    public CockroachDBValues(LinkedHashMap<CockroachDBColumn, List<CockroachDBConstant>> v) {
        this.values = v;
    }

    public LinkedHashMap<CockroachDBColumn, List<CockroachDBConstant>> getValues() {
        return this.values;
    }
}
