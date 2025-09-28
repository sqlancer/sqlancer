package sqlancer.tidb.ast;

import sqlancer.tidb.TiDBSchema.TiDBColumn;

public class TiDBColumnReference implements TiDBExpression {

    private final TiDBColumn c;
    private final TiDBConstant value;

    public TiDBColumnReference(TiDBColumn c, TiDBConstant value) {
        this.c = c;
        this.value = value;
    }

    public static TiDBColumnReference create(TiDBColumn column, TiDBConstant value) {
        return new TiDBColumnReference(column, value);
    }

    public TiDBColumn getColumn() {
        return c;
    }

    public TiDBConstant getValue() {
        return value;
    }

    @Override
    public TiDBConstant getExpectedValue() {
        return value;
    }
}
