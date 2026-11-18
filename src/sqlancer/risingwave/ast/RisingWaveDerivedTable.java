package sqlancer.risingwave.ast;

import java.util.List;

public final class RisingWaveDerivedTable extends RisingWaveRelation {
    public final RisingWaveSelect query;
    public final String alias;
    private final List<RisingWaveColumnReference> columns;

    public RisingWaveDerivedTable(RisingWaveSelect query, String alias) {
        this.query = query;
        this.alias = alias;
        columns = bindOutputs(query, alias);
    }

    @Override
    public List<RisingWaveColumnReference> columns() {
        return columns;
    }
}
