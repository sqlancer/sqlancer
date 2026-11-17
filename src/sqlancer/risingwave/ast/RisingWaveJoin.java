package sqlancer.risingwave.ast;

import java.util.ArrayList;
import java.util.List;

import sqlancer.common.ast.newast.Join;
import sqlancer.risingwave.RisingWaveSchema.RisingWaveColumn;
import sqlancer.risingwave.RisingWaveSchema.RisingWaveTable;
import sqlancer.risingwave.RisingWaveType;

public final class RisingWaveJoin extends RisingWaveRelation
        implements Join<RisingWaveExpression, RisingWaveTable, RisingWaveColumn> {
    public enum Kind {
        INNER, LEFT, RIGHT, FULL
    }

    public final RisingWaveRelation left;
    public final RisingWaveRelation right;
    public final Kind kind;
    public RisingWaveExpression condition;

    public RisingWaveJoin(RisingWaveRelation left, RisingWaveRelation right, Kind kind,
            RisingWaveExpression condition) {
        this.left = left;
        this.right = right;
        this.kind = kind;
        setOnClause(condition);
    }

    @Override
    public void setOnClause(RisingWaveExpression condition) {
        if (condition == null || condition.getType() != RisingWaveType.BOOLEAN) {
            throw new IllegalArgumentException("JOIN requires a boolean condition");
        }
        this.condition = condition;
    }

    @Override
    public List<RisingWaveColumnReference> columns() {
        List<RisingWaveColumnReference> columns = new ArrayList<>();
        left.columns().forEach(c -> columns.add(new RisingWaveColumnReference(c.qualifier, c.name, c.type,
                c.nullable || kind == Kind.RIGHT || kind == Kind.FULL)));
        right.columns().forEach(c -> columns.add(new RisingWaveColumnReference(c.qualifier, c.name, c.type,
                c.nullable || kind == Kind.LEFT || kind == Kind.FULL)));
        return columns;
    }
}
