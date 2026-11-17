package sqlancer.risingwave.ast;

import sqlancer.risingwave.RisingWaveType;

public final class RisingWaveFieldAccess implements RisingWaveExpression {
    public final RisingWaveExpression owner;
    public final int field;

    public RisingWaveFieldAccess(RisingWaveExpression owner, int field) {
        if (!owner.getType().struct() || field < 0 || field >= owner.getType().fields().size()) {
            throw new IllegalArgumentException("Unknown row field");
        }
        this.owner = owner;
        this.field = field;
    }

    @Override
    public RisingWaveType getType() {
        return owner.getType().fields().get(field).type;
    }
}
