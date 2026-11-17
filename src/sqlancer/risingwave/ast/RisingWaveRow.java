package sqlancer.risingwave.ast;

import java.util.List;

import sqlancer.risingwave.RisingWaveType;

public final class RisingWaveRow implements RisingWaveExpression {
    public final RisingWaveType type;
    public final List<RisingWaveExpression> fields;

    public RisingWaveRow(RisingWaveType type, List<RisingWaveExpression> fields) {
        if (!type.struct() || fields.size() != type.fields().size()) {
            throw new IllegalArgumentException("Incorrect row arity");
        }
        for (int i = 0; i < fields.size(); i++) {
            if (fields.get(i).getType() != type.fields().get(i).type) {
                throw new IllegalArgumentException("Incorrect row field type");
            }
        }
        this.type = type;
        this.fields = List.copyOf(fields);
    }

    @Override
    public RisingWaveType getType() {
        return type;
    }
}
