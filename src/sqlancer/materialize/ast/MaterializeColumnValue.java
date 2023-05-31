package sqlancer.materialize.ast;

import sqlancer.materialize.MaterializeSchema.MaterializeColumn;
import sqlancer.materialize.MaterializeSchema.MaterializeDataType;

public class MaterializeColumnValue implements MaterializeExpression {

    private final MaterializeColumn c;
    private final MaterializeConstant expectedValue;

    public MaterializeColumnValue(MaterializeColumn c, MaterializeConstant expectedValue) {
        this.c = c;
        this.expectedValue = expectedValue;
    }

    @Override
    public MaterializeDataType getExpressionType() {
        return c.getType();
    }

    @Override
    public MaterializeConstant getExpectedValue() {
        return expectedValue;
    }

    public static MaterializeColumnValue create(MaterializeColumn c, MaterializeConstant expected) {
        return new MaterializeColumnValue(c, expected);
    }

    public MaterializeColumn getColumn() {
        return c;
    }

}
