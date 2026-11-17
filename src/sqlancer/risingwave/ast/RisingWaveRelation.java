package sqlancer.risingwave.ast;

import java.util.List;
import java.util.stream.Collectors;

import sqlancer.risingwave.RisingWaveType;

public abstract class RisingWaveRelation implements RisingWaveExpression {
    public abstract List<RisingWaveColumnReference> columns();

    static List<RisingWaveColumnReference> bindOutputs(RisingWaveSelect query, String alias) {
        return query.getFetchColumns().stream().map(item -> {
            if (!(item instanceof RisingWaveAlias)) {
                throw new IllegalArgumentException("Relation outputs require explicit names");
            }
            RisingWaveAlias output = (RisingWaveAlias) item;
            boolean nullable = true;
            if (output.expression instanceof RisingWaveColumnReference) {
                RisingWaveColumnReference column = (RisingWaveColumnReference) output.expression;
                nullable = column.nullable || query.getGroupingSets().stream().anyMatch(s -> !s.contains(column));
            }
            return new RisingWaveColumnReference(alias, output.name, output.getType(), nullable);
        }).collect(Collectors.toUnmodifiableList());
    }

    @Override
    public final RisingWaveType getType() {
        throw new IllegalStateException("A relation is not a scalar");
    }
}
