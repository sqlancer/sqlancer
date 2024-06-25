package sqlancer.cnosdb.ast;

import sqlancer.cnosdb.CnosDBSchema.CnosDBColumn;
import sqlancer.cnosdb.CnosDBSchema.CnosDBDataType;

public class CnosDBColumnValue extends CnosDBColumnReference implements CnosDBExpression {

    private CnosDBConstant expectedValue;

    public CnosDBColumnValue(CnosDBColumn c, CnosDBConstant value) {
        super(c);
        this.expectedValue = value;
    
    }



    @Override
    public CnosDBConstant getExpectedValue() {
        return expectedValue;
    }

    @Override
    public CnosDBDataType getExpressionType() {
        return getColumn().getType();
    }

    public static CnosDBColumnValue create(CnosDBColumn c, CnosDBConstant value) {
        return new CnosDBColumnValue(c, value);
    }


}
