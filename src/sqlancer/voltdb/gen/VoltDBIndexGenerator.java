package sqlancer.voltdb.gen;

import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.voltdb.VoltDBErrors;
import sqlancer.voltdb.VoltDBProvider.VoltDBGlobalState;
import sqlancer.voltdb.VoltDBSchema.VoltDBColumn;
import sqlancer.voltdb.VoltDBSchema.VoltDBTable;

/**
 * CREATE [UNIQUE | ASSUMEUNIQUE] INDEX over a random column subset. As with table constraints, a unique index on a
 * partitioned table that does not include the partitioning column is ASSUMEUNIQUE.
 */
public final class VoltDBIndexGenerator {

    private VoltDBIndexGenerator() {
    }

    public static SQLQueryAdapter getQuery(VoltDBGlobalState globalState) {
        VoltDBTable table = globalState.getSchema().getRandomTable();
        List<VoltDBColumn> columns = Randomly.nonEmptySubset(table.getColumns());
        StringBuilder sb = new StringBuilder("CREATE ");
        if (Randomly.getBooleanWithRatherLowProbability()) {
            boolean hasPartitionColumn = !table.isPartitioned()
                    || columns.stream().anyMatch(c -> c.getName().equals(table.getPartitionColumn()));
            if (hasPartitionColumn) {
                sb.append("UNIQUE ");
            } else if (!globalState.getDbmsSpecificOptions().resizes) {
                // no ASSUMEUNIQUE index while the cluster resizes (see VoltDBTableGenerator)
                sb.append("ASSUMEUNIQUE ");
            }
        }
        sb.append("INDEX ").append(globalState.getFreeIndexName()).append(" ON ").append(table.getName()).append(" (")
                .append(columns.stream().map(VoltDBColumn::getName).collect(Collectors.joining(", "))).append(')');
        ExpectedErrors errors = ExpectedErrors.newErrors().build();
        VoltDBErrors.addIndexErrors(errors);
        return new SQLQueryAdapter(sb.toString(), errors, true);
    }

}
