package sqlancer.doris.ast;

import java.util.Objects;

import sqlancer.common.ast.newast.ColumnReferenceNode;
import sqlancer.doris.DorisSchema.DorisColumn;
import sqlancer.doris.DorisSchema.DorisDataType;

public class DorisColumnValue extends ColumnReferenceNode<DorisExpression, DorisColumn> implements DorisExpression {

    private final DorisConstant expectedValue;

    public DorisColumnValue(DorisColumn column, DorisConstant value) {
        super(column);
        this.expectedValue = value;
    }

    @Override
    public DorisConstant getExpectedValue() {
        return expectedValue;
    }

    @Override
    public DorisDataType getExpectedType() {
        return getColumn().getType().getPrimitiveDataType();
    }

    public static DorisColumnValue create(DorisColumn column, DorisConstant value) {
        return new DorisColumnValue(column, value);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DorisColumnValue that = (DorisColumnValue) o;
        if (!this.getColumn().getName().equals(that.getColumn().getName())) {
            return false;
        }
        return Objects.equals(expectedValue, that.expectedValue);
    }

    @Override
    public int hashCode() {
        String nameAndValue = this.getColumn().getName();
        nameAndValue += expectedValue == null ? "NULL" : expectedValue.toString();
        return Objects.hash(nameAndValue);
    }
}
