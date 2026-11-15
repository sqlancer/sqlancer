package sqlancer.voltdb.gen;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.voltdb.VoltDBProvider.VoltDBGlobalState;
import sqlancer.voltdb.VoltDBSchema.VoltDBCompositeDataType;
import sqlancer.voltdb.VoltDBSchema.VoltDBDataType;

/**
 * CREATE TABLE, followed by PARTITION TABLE for about half the tables. Both go to VoltDB as one batch, so a table is
 * never seen replicated before it is partitioned.
 *
 * <p>
 * VoltDB rules the generator follows: the partitioning column is NOT NULL and of an integer, VARCHAR or VARBINARY type,
 * and a PRIMARY KEY or UNIQUE constraint on a partitioned table includes the partitioning column (otherwise the
 * constraint is ASSUMEUNIQUE, which VoltDB enforces only within a partition).
 */
public final class VoltDBTableGenerator {

    private VoltDBTableGenerator() {
    }

    private static final class Column {
        private final String name;
        private final VoltDBCompositeDataType type;
        private boolean notNull;

        Column(String name, VoltDBCompositeDataType type, boolean notNull) {
            this.name = name;
            this.type = type;
            this.notNull = notNull;
        }
    }

    public static SQLQueryAdapter getQuery(VoltDBGlobalState globalState) {
        String table = globalState.getFreeTableName();
        List<Column> columns = new ArrayList<>();
        int nrColumns = (int) Randomly.getNotCachedInteger(1, 7);
        // ENG-30255 (fixed after 16.0.0): grouping, DISTINCT or a set operation over a DECIMAL that is NULL aborts a
        // 16.0.0 server (NValue::hashCombine). During an in-service upgrade from 16.0.0 (--upgrades) the cluster runs
        // 16.0.0 nodes for a while, so its tables have no DECIMAL columns
        boolean noDecimal = globalState.getDbmsSpecificOptions().upgrades
                && globalState.getDbmsSpecificOptions().avoidKnownBugs;
        for (int i = 0; i < nrColumns; i++) {
            VoltDBDataType type;
            do {
                type = VoltDBDataType.getRandom();
            } while (noDecimal && type == VoltDBDataType.DECIMAL);
            columns.add(new Column("c" + i, VoltDBCompositeDataType.getRandom(type),
                    Randomly.getBooleanWithRatherLowProbability()));
        }

        Column partitionColumn = null;
        if (Randomly.getBoolean()) {
            int i = (int) Randomly.getNotCachedInteger(0, columns.size());
            // ENG-30610: a single-partition query whose VARBINARY partitioning value is an inline literal (a join with
            // a constant ON condition is not auto-parameterized) runs on the wrong partition, so unless
            // --avoid-known-bugs is false no table is partitioned on a VARBINARY column
            boolean avoid = globalState.getDbmsSpecificOptions().avoidKnownBugs;
            VoltDBDataType type = columns.get(i).type.getType();
            if (!type.isPartitionable() || avoid && type == VoltDBDataType.VARBINARY) {
                VoltDBDataType partitionType;
                do {
                    partitionType = VoltDBDataType.getRandomPartitionable();
                } while (avoid && partitionType == VoltDBDataType.VARBINARY);
                columns.set(i, new Column(columns.get(i).name, VoltDBCompositeDataType.getRandom(partitionType), true));
            }
            partitionColumn = columns.get(i);
            partitionColumn.notNull = true;
        }

        List<Column> primaryKey = null;
        if (Randomly.getBooleanWithRatherLowProbability() || Randomly.getBoolean()) {
            primaryKey = keyColumns(columns, partitionColumn, true);
            if (globalState.getDbmsSpecificOptions().avoidKnownBugs) {
                // ENG-30514: VoltDB stores NULL in a primary key column that is not declared NOT NULL, but its
                // planner assumes such a column is never NULL, which gives wrong rows for outer joins
                primaryKey.forEach(c -> c.notNull = true);
            }
        }
        List<Column> uniqueKey = Randomly.getBooleanWithRatherLowProbability()
                ? keyColumns(columns, partitionColumn, false) : null;
        // ASSUMEUNIQUE holds only within a partition, so rows in two partitions can share its key; an elastic resize
        // (--resizes) that moves one of them next to the other fails with "Found duplicates while rebalancing data
        // across partitions" and retries for good, as VoltDB documents. The key is left out then.
        if (uniqueKey != null && globalState.getDbmsSpecificOptions().resizes && partitionColumn != null
                && !uniqueKey.contains(partitionColumn)) {
            uniqueKey = null;
        }

        StringBuilder sb = new StringBuilder("CREATE TABLE ").append(table).append(" (");
        sb.append(columns.stream().map(c -> c.name + " " + c.type + (c.notNull ? " NOT NULL" : ""))
                .collect(Collectors.joining(", ")));
        if (primaryKey != null) {
            appendKey(sb, primaryKey, partitionColumn, true);
        }
        if (uniqueKey != null) {
            appendKey(sb, uniqueKey, partitionColumn, false);
        }
        sb.append(");");
        if (partitionColumn != null) {
            sb.append(" PARTITION TABLE ").append(table).append(" ON COLUMN ").append(partitionColumn.name).append(';');
        }
        return new SQLQueryAdapter(sb.toString(), ExpectedErrors.newErrors().build(), true);
    }

    // A random column subset for a key; a primary key always includes the partitioning column.
    private static List<Column> keyColumns(List<Column> columns, Column partitionColumn, boolean primary) {
        List<Column> key = new ArrayList<>(Randomly.nonEmptySubset(columns));
        if (primary && partitionColumn != null && !key.contains(partitionColumn)) {
            key.add(partitionColumn);
        }
        return key;
    }

    // Appends PRIMARY KEY (primary) or a UNIQUE / ASSUMEUNIQUE constraint over the key columns.
    private static void appendKey(StringBuilder sb, List<Column> key, Column partitionColumn, boolean primary) {
        boolean hasPartitionColumn = partitionColumn == null || key.contains(partitionColumn);
        sb.append(", ");
        if (primary) {
            sb.append("PRIMARY KEY");
        } else {
            sb.append(hasPartitionColumn ? "UNIQUE" : "ASSUMEUNIQUE");
        }
        sb.append(" (").append(key.stream().map(c -> c.name).collect(Collectors.joining(", "))).append(')');
    }

}
