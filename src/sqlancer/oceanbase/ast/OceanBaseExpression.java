package sqlancer.oceanbase.ast;

import sqlancer.common.ast.newast.Expression;
import sqlancer.oceanbase.OceanBaseSchema.OceanBaseColumn;

public interface OceanBaseExpression extends Expression<OceanBaseColumn> {

    default OceanBaseConstant getExpectedValue() {
        throw new AssertionError("PQS not supported for this operator");
    }

}
