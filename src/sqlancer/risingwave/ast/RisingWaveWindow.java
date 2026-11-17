package sqlancer.risingwave.ast;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.risingwave.RisingWaveType;

public final class RisingWaveWindow extends RisingWaveRelation {
    public final RisingWaveTableReference table;
    public final String timestampColumn;
    public final String alias;
    public final boolean hop;
    public final int slideSeconds;
    public final int sizeSeconds;
    private final List<RisingWaveColumnReference> columns;

    public RisingWaveWindow(RisingWaveTableReference table, String timestampColumn, String alias, boolean hop,
            int slideSeconds, int sizeSeconds) {
        if (table.columns().stream()
                .noneMatch(c -> c.name.equals(timestampColumn) && c.type == RisingWaveType.TIMESTAMP)) {
            throw new IllegalArgumentException("Time window requires a timestamp column");
        }
        if (slideSeconds < 1 || sizeSeconds < slideSeconds || sizeSeconds % slideSeconds != 0
                || sizeSeconds / slideSeconds > 4 || !hop && sizeSeconds != slideSeconds) {
            throw new IllegalArgumentException("Invalid or unbounded time-window expansion");
        }
        this.slideSeconds = slideSeconds;
        this.sizeSeconds = sizeSeconds;
        this.table = table;
        this.timestampColumn = timestampColumn;
        this.alias = alias;
        this.hop = hop;
        columns = table.columns().stream().map(c -> new RisingWaveColumnReference(alias, c.name, c.type))
                .collect(Collectors.toList());
        columns.add(new RisingWaveColumnReference(alias, "window_start", RisingWaveType.TIMESTAMP));
        columns.add(new RisingWaveColumnReference(alias, "window_end", RisingWaveType.TIMESTAMP));
    }

    @Override
    public List<RisingWaveColumnReference> columns() {
        return Collections.unmodifiableList(columns);
    }
}
