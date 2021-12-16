package sqlancer.oceanbase.ast;

import sqlancer.oceanbase.OceanBaseSchema.OceanBaseColumn;

public class OceanBaseColumnReference implements OceanBaseExpression {

    private final OceanBaseColumn column;
    private final OceanBaseConstant value;
    private boolean isRef;

    public OceanBaseColumnReference(OceanBaseColumn column, OceanBaseConstant value) {
        this.column = column;
        this.value = value;
    }

    public static OceanBaseColumnReference create(OceanBaseColumn column, OceanBaseConstant value) {
        return new OceanBaseColumnReference(column, value);
    }

    public OceanBaseColumn getColumn() {
        return column;
    }

    public OceanBaseConstant getValue() {
        return value;
    }

    @Override
    public OceanBaseConstant getExpectedValue() {
        return value;
    }

    public OceanBaseColumnReference setRef(boolean isRef) {
        this.isRef = isRef;
        return this;
    }

    public boolean getRef() {
        return isRef;
    }

}
