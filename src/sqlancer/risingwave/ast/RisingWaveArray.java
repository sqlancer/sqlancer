package sqlancer.risingwave.ast;

import java.util.List;

import sqlancer.risingwave.RisingWaveType;

public final class RisingWaveArray implements RisingWaveExpression {
    public final RisingWaveType type;
    public final List<RisingWaveExpression> elements;

    public RisingWaveArray(RisingWaveType type, List<RisingWaveExpression> elements) {
        if (!type.array() || elements.stream().anyMatch(e -> e.getType() != type.elementType())) {
            throw new IllegalArgumentException("Invalid array constructor");
        }
        this.type = type;
        this.elements = List.copyOf(elements);
    }

    @Override
    public RisingWaveType getType() {
        return type;
    }
}
