package sqlancer.tidb.ast;

import sqlancer.common.ast.newast.Expression;
import sqlancer.tidb.TiDBSchema.TiDBColumn;

public interface TiDBExpression extends Expression<TiDBColumn> {
    default TiDBConstant getExpectedValue() {
        throw new AssertionError("PQS not supported for this operator");
    }
}
