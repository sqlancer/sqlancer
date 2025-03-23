package sqlancer.tidb.ast;

import java.util.LinkedHashMap;
import java.util.List;

import sqlancer.tidb.TiDBSchema.TiDBColumn;

public class TiDBValuesRow implements TiDBExpression {
    private final LinkedHashMap<TiDBColumn, List<TiDBConstant>> values;

    public TiDBValuesRow(LinkedHashMap<TiDBColumn, List<TiDBConstant>> values) {
        this.values = values;
    }

    public LinkedHashMap<TiDBColumn, List<TiDBConstant>> getValues() {
        return this.values;
    }
}
