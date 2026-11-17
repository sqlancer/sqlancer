package sqlancer.risingwave.ast;

import sqlancer.risingwave.RisingWaveType;

public final class RisingWaveConstant implements RisingWaveExpression {
    public final RisingWaveType type;
    public final String value;

    public RisingWaveConstant(RisingWaveType type, String value) {
        if (type.array() && value != null) {
            throw new IllegalArgumentException("Use a typed array constructor for non-NULL arrays");
        }
        this.type = type;
        this.value = value;
    }

    @Override
    public RisingWaveType getType() {
        return type;
    }
}
