package sqlancer.databend.ast;

import sqlancer.databend.DatabendSchema.DatabendDataType;

public interface DatabendExpression {

    default DatabendDataType getExpectedType() {
        return null;
    }

    default DatabendConstant getExpectedValue() {
        return null;
    }
}
