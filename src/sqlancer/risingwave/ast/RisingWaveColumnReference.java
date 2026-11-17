package sqlancer.risingwave.ast;

import sqlancer.risingwave.RisingWaveType;

public final class RisingWaveColumnReference implements RisingWaveExpression {
    public final String qualifier;
    public final String name;
    public final RisingWaveType type;
    public final boolean nullable;

    public RisingWaveColumnReference(String qualifier, String name, RisingWaveType type) {
        this(qualifier, name, type, true);
    }

    public RisingWaveColumnReference(String qualifier, String name, RisingWaveType type, boolean nullable) {
        this.qualifier = qualifier;
        this.name = name;
        this.type = type;
        this.nullable = nullable;
    }

    @Override
    public RisingWaveType getType() {
        return type;
    }
}
