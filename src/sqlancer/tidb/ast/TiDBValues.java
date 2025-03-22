package sqlancer.tidb.ast;

import java.util.LinkedHashMap;
import java.util.List;

import sqlancer.tidb.TiDBSchema.TiDBColumn;

public class TiDBValues implements TiDBExpression {

    private final LinkedHashMap<TiDBColumn, List<TiDBConstant>> values;

    public TiDBValues(LinkedHashMap<TiDBColumn, List<TiDBConstant>> v) {
        this.values = v;
    }

    public LinkedHashMap<TiDBColumn, List<TiDBConstant>> getValues() {
        return this.values;
    }
}
