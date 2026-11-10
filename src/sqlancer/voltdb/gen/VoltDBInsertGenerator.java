package sqlancer.voltdb.gen;

import java.util.ArrayList;
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
 * Single-row INSERT, or UPSERT into a table with a primary key (VoltDB supports UPSERT only there). VoltDB has no
 * multi-row VALUES, and every NOT NULL column, which includes the primary key's, is always given a value.
 */
public final class VoltDBInsertGenerator {

    private VoltDBInsertGenerator() {
    }

    public static SQLQueryAdapter getQuery(VoltDBGlobalState globalState) {
        return getQuery(globalState, globalState.getSchema().getRandomTable());
    }

    public static SQLQueryAdapter getQuery(VoltDBGlobalState globalState, VoltDBTable table) {
        List<VoltDBColumn> columns = new ArrayList<>();
        for (VoltDBColumn c : table.getColumns()) {
            if (!c.isNullable() || Randomly.getBoolean()) {
                columns.add(c);
            }
        }
        if (columns.isEmpty()) {
            columns.add(Randomly.fromList(table.getColumns()));
        }
        Randomly r = globalState.getRandomly();
        String verb = !table.getPrimaryKey().isEmpty() && Randomly.getBooleanWithRatherLowProbability() ? "UPSERT"
                : "INSERT";
        String sql = verb + " INTO " + table.getName() + " ("
                + columns.stream().map(VoltDBColumn::getName).collect(Collectors.joining(", ")) + ") VALUES ("
                + columns.stream().map(c -> VoltDBConstantGenerator.generate(r, c.getType(), c.isNullable()))
                        .collect(Collectors.joining(", "))
                + ")";
        ExpectedErrors errors = ExpectedErrors.newErrors().build();
        VoltDBErrors.addInsertErrors(errors);
        return new SQLQueryAdapter(sql, errors);
    }

}
