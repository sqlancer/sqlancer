package sqlancer.doris.ast;

import sqlancer.common.ast.newast.Expression;
import sqlancer.doris.DorisSchema;
import sqlancer.doris.DorisSchema.DorisColumn;

public interface DorisExpression extends Expression<DorisColumn> {
    default DorisSchema.DorisDataType getExpectedType() {
        return null;
    }

    default DorisConstant getExpectedValue() {
        return null;
    }
}
