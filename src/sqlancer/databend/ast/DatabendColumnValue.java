package sqlancer.databend.ast;

import sqlancer.common.ast.newast.ColumnReferenceNode;
import sqlancer.databend.DatabendSchema.DatabendColumn;
import sqlancer.databend.DatabendSchema.DatabendDataType;

public class DatabendColumnValue extends ColumnReferenceNode<DatabendExpression, DatabendColumn>
        implements DatabendExpression {

    private final DatabendConstant expectedValue;

    public DatabendColumnValue(DatabendColumn column, DatabendConstant value) {
        super(column);
        this.expectedValue = value;
    }

    @Override
    public DatabendConstant getExpectedValue() {
        return expectedValue;
    }

    @Override
    public DatabendDataType getExpectedType() {
        return getColumn().getType().getPrimitiveDataType();
    }

    public static DatabendColumnValue create(DatabendColumn column, DatabendConstant value) {
        return new DatabendColumnValue(column, value);
    }

}
