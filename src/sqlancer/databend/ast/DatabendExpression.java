package sqlancer.databend.ast;

import sqlancer.common.ast.newast.Expression;
import sqlancer.databend.DatabendSchema.DatabendColumn;
import sqlancer.databend.DatabendSchema.DatabendDataType;

public interface DatabendExpression extends Expression<DatabendColumn> {

    default DatabendDataType getExpectedType() {
        return null;
    }

    default DatabendConstant getExpectedValue() {
        return null;
    }
}
