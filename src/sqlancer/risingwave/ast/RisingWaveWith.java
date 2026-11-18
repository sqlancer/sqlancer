package sqlancer.risingwave.ast;

import java.util.List;

/** Ordered nonrecursive CTE definitions and a consumer query, enclosed in a derived relation. */
public final class RisingWaveWith extends RisingWaveRelation {
    public final List<Definition> definitions;
    public final RisingWaveSelect consumer;
    public final String alias;
    private final List<RisingWaveColumnReference> columns;

    public static final class Definition {
        public final String name;
        public final RisingWaveSelect query;

        public Definition(String name, RisingWaveSelect query) {
            this.name = name;
            this.query = query;
        }
    }

    public RisingWaveWith(List<Definition> definitions, RisingWaveSelect consumer, String alias) {
        this.definitions = List.copyOf(definitions);
        this.consumer = consumer;
        this.alias = alias;
        columns = new RisingWaveDerivedTable(consumer, alias).columns();
    }

    @Override
    public List<RisingWaveColumnReference> columns() {
        return columns;
    }
}
