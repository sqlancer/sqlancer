package sqlancer.risingwave.ast;

import sqlancer.risingwave.RisingWaveType;

public final class RisingWaveAlias implements RisingWaveExpression {
    public final RisingWaveExpression expression;
    public final String name;

    public RisingWaveAlias(RisingWaveExpression expression, String name) {
        this.expression = expression;
        this.name = name;
    }

    @Override
    public RisingWaveType getType() {
        return expression.getType();
    }
}
