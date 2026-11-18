package sqlancer.risingwave.ast;

import sqlancer.risingwave.RisingWaveType;

public final class RisingWaveCase implements RisingWaveExpression {
    public final RisingWaveExpression condition;
    public final RisingWaveExpression thenExpression;
    public final RisingWaveExpression elseExpression;

    public RisingWaveCase(RisingWaveExpression condition, RisingWaveExpression thenExpression,
            RisingWaveExpression elseExpression) {
        if (condition.getType() != RisingWaveType.BOOLEAN || thenExpression.getType() != elseExpression.getType()) {
            throw new IllegalArgumentException("CASE requires a boolean condition and matching branches");
        }
        this.condition = condition;
        this.thenExpression = thenExpression;
        this.elseExpression = elseExpression;
    }

    @Override
    public RisingWaveType getType() {
        return thenExpression.getType();
    }
}
