package sqlancer.cnosdb.ast;

import sqlancer.cnosdb.CnosDBSchema.CnosDBColumn;
import sqlancer.cnosdb.CnosDBSchema.CnosDBDataType;

public class CnosDBColumnValue implements CnosDBExpression {

    private final CnosDBColumn c;
    private final CnosDBConstant expectedValue;

    public CnosDBColumnValue(CnosDBColumn c, CnosDBConstant expectedValue) {
        this.c = c;
        this.expectedValue = expectedValue;
    }

    @Override
    public CnosDBDataType getExpressionType() {
        return c.getType();
    }

    @Override
    public CnosDBConstant getExpectedValue() {
        return expectedValue;
    }

    public static CnosDBColumnValue create(CnosDBColumn c, CnosDBConstant expected) {
        return new CnosDBColumnValue(c, expected);
    }

    public CnosDBColumn getColumn() {
        return c;
    }

}
