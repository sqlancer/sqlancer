package sqlancer.risingwave.ast;

import sqlancer.risingwave.RisingWaveType;

public final class RisingWaveCast implements RisingWaveExpression {
    public final RisingWaveExpression expression;
    private final RisingWaveType type;

    public RisingWaveCast(RisingWaveExpression expression, RisingWaveType type) {
        this.expression = expression;
        this.type = type;
    }

    @Override
    public RisingWaveType getType() {
        return type;
    }
}
