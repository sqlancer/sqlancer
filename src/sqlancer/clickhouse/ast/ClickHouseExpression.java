package sqlancer.clickhouse.ast;

import sqlancer.clickhouse.ClickHouseSchema.ClickHouseColumn;
import sqlancer.common.ast.newast.Expression;

public interface ClickHouseExpression extends Expression<ClickHouseColumn> {

    default ClickHouseConstant getExpectedValue() {
        return null;
    }

}
