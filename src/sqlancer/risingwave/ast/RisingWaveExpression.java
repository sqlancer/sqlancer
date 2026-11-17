package sqlancer.risingwave.ast;

import sqlancer.common.ast.newast.Expression;
import sqlancer.risingwave.RisingWaveSchema.RisingWaveColumn;
import sqlancer.risingwave.RisingWaveType;

public interface RisingWaveExpression extends Expression<RisingWaveColumn> {
    RisingWaveType getType();
}
