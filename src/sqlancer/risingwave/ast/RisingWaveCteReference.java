package sqlancer.risingwave.ast;

import java.util.List;

/** A locally bound CTE consumer; every reference has its own qualified output scope. */
public final class RisingWaveCteReference extends RisingWaveRelation {
    public final String name;
    public final String alias;
    private final List<RisingWaveColumnReference> columns;

    public RisingWaveCteReference(String name, String alias, RisingWaveSelect definition) {
        this.name = name;
        this.alias = alias;
        columns = bindOutputs(definition, alias);
    }

    @Override
    public List<RisingWaveColumnReference> columns() {
        return columns;
    }
}
