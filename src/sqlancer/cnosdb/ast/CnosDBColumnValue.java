package sqlancer.cnosdb.ast;

import sqlancer.cnosdb.CnosDBSchema.CnosDBColumn;
import sqlancer.cnosdb.CnosDBSchema.CnosDBDataType;

public class CnosDBColumnValue implements CnosDBExpression {

    private final CnosDBColumn c;

    public CnosDBColumnValue(CnosDBColumn c) {
        this.c = c;
    }

    public static CnosDBColumnValue create(CnosDBColumn c) {
        return new CnosDBColumnValue(c);
    }

    @Override
    public CnosDBDataType getExpressionType() {
        return c.getType();
    }

    public CnosDBColumn getColumn() {
        return c;
    }

}
