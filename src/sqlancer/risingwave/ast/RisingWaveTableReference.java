package sqlancer.risingwave.ast;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.risingwave.RisingWaveSchema.RisingWaveTable;

public final class RisingWaveTableReference extends RisingWaveRelation {
    public final String name;
    public final String alias;
    public final sqlancer.risingwave.RisingWaveSchema.RelationKind kind;
    private final List<RisingWaveColumnReference> columns;

    public RisingWaveTableReference(RisingWaveTable table, String alias) {
        this.name = table.getName();
        this.alias = alias;
        this.kind = table.getKind();
        columns = table.getColumns().stream()
                .map(c -> new RisingWaveColumnReference(alias, c.getName(), c.getType(), c.isNullable()))
                .collect(Collectors.toList());
    }

    @Override
    public List<RisingWaveColumnReference> columns() {
        return Collections.unmodifiableList(columns);
    }
}
