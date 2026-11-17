package sqlancer.risingwave.ast;

import sqlancer.risingwave.RisingWaveType;

public final class RisingWaveArrayAccess implements RisingWaveExpression {
    public final RisingWaveExpression array;
    public final RisingWaveExpression index;

    public RisingWaveArrayAccess(RisingWaveExpression array, RisingWaveExpression index) {
        if (!array.getType().array() || index.getType() != RisingWaveType.INT) {
            throw new IllegalArgumentException("Array access requires an array and INTEGER index");
        }
        this.array = array;
        this.index = index;
    }

    @Override
    public RisingWaveType getType() {
        return array.getType().elementType();
    }
}
