package sqlancer.voltdb.oracle;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.Reproducer;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.schema.AbstractTables;
import sqlancer.voltdb.VoltDBProvider.VoltDBGlobalState;
import sqlancer.voltdb.VoltDBSchema.VoltDBColumn;
import sqlancer.voltdb.VoltDBSchema.VoltDBDataType;
import sqlancer.voltdb.VoltDBSchema.VoltDBTable;
import sqlancer.voltdb.ast.VoltDBExpression;
import sqlancer.voltdb.ast.VoltDBSelect;
import sqlancer.voltdb.gen.VoltDBExpressionGenerator;

/**
 * Schema changes in the middle of a run, and the plan cache. A change to a table that does not touch the columns a
 * query reads must not change the query's rows. The oracle writes up to {@link #QUERIES} queries over one table and
 * runs each twice, the second time from VoltDB's plan cache. Then it changes the table, runs them again, which needs
 * new plans because the catalog changed, undoes the change, and runs them a fourth time. All four runs must return the
 * same rows. The change is one of:
 * <ul>
 * <li>{@code CREATE INDEX} on one to three of the table's columns (undone by {@code DROP INDEX}), which can change
 * every plan that reads the table;</li>
 * <li>{@code ALTER TABLE t ADD COLUMN x type [DEFAULT d]} (undone by {@code DROP COLUMN}), which moves no existing
 * column but rewrites every row; every row's new value must be d, or NULL without a DEFAULT;</li>
 * <li>{@code ALTER TABLE t ALTER COLUMN c VARCHAR(n)} with a larger n, on a VARCHAR column outside the primary key and
 * the partitioning column (not undone), which can move the column's values out of the tuple when n crosses 63
 * bytes.</li>
 * </ul>
 * VoltDB applies one catalog update at a time for the whole cluster, blocking every partition, so the oracle changes
 * the schema once per database (its first check) and skips its later checks.
 *
 * <p>
 * If VoltDB refuses a change (tables with a materialized view are left out, as it refuses most changes to them), the
 * check is skipped, as this oracle checks query results, not which changes are allowed. A failure has no reproducer, as
 * reducing the database's statements would lose the order of the queries and the change; its message holds the change
 * and the query.
 */
public class VoltDBSchemaChangeOracle implements TestOracle<VoltDBGlobalState> {

    private enum Change {
        INDEX, ADD_COLUMN, WIDEN
    }

    private static final int QUERIES = 8;

    private final VoltDBGlobalState state;
    private final ExpectedErrors errors;
    private boolean done;
    private String lastQuery;
    private Reproducer<VoltDBGlobalState> reproducer;

    public VoltDBSchemaChangeOracle(VoltDBGlobalState state, ExpectedErrors errors) {
        this.state = state;
        this.errors = errors;
    }

    @Override
    public void check() throws Exception {
        reproducer = null;
        // ENG-30612: a node killed while an @UpdateCore is in flight can wedge the MPI for good (the blocker it held is
        // reclaimed before its update is resolved), and this oracle's ALTER TABLE rewrites every row, which makes the
        // update slow and such a kill likely; unless --avoid-known-bugs is false it does nothing while nodes fail
        if (done || state.getDbmsSpecificOptions().nodeFailures && state.getDbmsSpecificOptions().avoidKnownBugs) {
            throw new IgnoreMeException();
        }
        done = true;
        // tables with a materialized view are left out: VoltDB refuses most changes to them, and ENG-30582 limits the
        // queries that read them
        Set<String> viewed = state.getViews().values().stream().map(v -> v[0]).collect(Collectors.toSet());
        List<VoltDBTable> tables = state.getSchema().getDatabaseTables().stream()
                .filter(t -> !viewed.contains(t.getName())).collect(Collectors.toList());
        if (tables.isEmpty()) {
            throw new IgnoreMeException();
        }
        VoltDBTable table = Randomly.fromList(tables);

        List<String> queries = new ArrayList<>();
        List<Integer> widths = new ArrayList<>();
        for (int i = 0; i < QUERIES; i++) {
            VoltDBExpressionGenerator gen = new VoltDBExpressionGenerator(state);
            gen.setTablesAndColumns(new AbstractTables<>(List.of(table)));
            VoltDBSelect select = gen.generateSelect();
            select.setFromList(gen.getTableRefs());
            List<VoltDBExpression> fetch = gen.generateFetchColumns();
            select.setFetchColumns(fetch);
            if (Randomly.getBoolean()) {
                select.setWhereClause(gen.generateBooleanExpression());
            }
            queries.add(select.asString());
            widths.add(fetch.size());
        }

        String[] change = change(table);
        String described = "-- change: " + change[0] + ';' + System.lineSeparator() + "-- undo: "
                + (change[1] == null ? "none" : change[1] + ';') + System.lineSeparator();

        // the rows before, from a new plan and from the cached one; a query with an expected error is dropped
        List<String> kept = new ArrayList<>();
        List<Integer> keptWidths = new ArrayList<>();
        List<List<String>> before = new ArrayList<>();
        for (int i = 0; i < queries.size(); i++) {
            try {
                List<String> first = rows(queries.get(i), widths.get(i));
                List<String> cached = rows(queries.get(i), widths.get(i));
                compare(queries.get(i), "the query run again (its cached plan)", first, cached, described);
                kept.add(queries.get(i));
                keptWidths.add(widths.get(i));
                before.add(first);
            } catch (IgnoreMeException e) {
                // the next query
            }
        }
        if (kept.isEmpty()) {
            throw new IgnoreMeException();
        }

        if (!ddl(change[0])) {
            throw new IgnoreMeException();
        }
        try {
            state.updateSchema();
            if (change.length > 2) {
                checkNewColumn(table, change, described);
            }
            for (int i = 0; i < kept.size(); i++) {
                compare(kept.get(i), "after " + change[0], before.get(i), rows(kept.get(i), keptWidths.get(i)),
                        described);
            }
        } finally {
            if (change[1] != null) {
                ddl(change[1]);
            }
            state.updateSchema();
        }
        if (change[1] != null) {
            for (int i = 0; i < kept.size(); i++) {
                compare(kept.get(i), "after " + change[1], before.get(i), rows(kept.get(i), keptWidths.get(i)),
                        described);
            }
        }
    }

    // {the change, its undo or null, and for ADD COLUMN the new column and the condition every row must meet}
    private String[] change(VoltDBTable table) {
        List<VoltDBColumn> widenable = table.getColumns().stream()
                .filter(c -> c.getType().getType() == VoltDBDataType.VARCHAR
                        && !c.getName().equals(table.getPartitionColumn())
                        && !table.getPrimaryKey().contains(c.getName()))
                .collect(Collectors.toList());
        List<Change> options = new ArrayList<>(List.of(Change.INDEX, Change.ADD_COLUMN));
        if (!widenable.isEmpty()) {
            options.add(Change.WIDEN);
        }
        String name = table.getName();
        switch (Randomly.fromList(options)) {
        case INDEX:
            String index = state.getFreeIndexName();
            List<VoltDBColumn> columns = Randomly.nonEmptySubset(table.getColumns());
            String list = columns.subList(0, Math.min(3, columns.size())).stream().map(VoltDBColumn::getName)
                    .collect(Collectors.joining(", "));
            return new String[] { "CREATE INDEX " + index + " ON " + name + " (" + list + ")",
                    "DROP INDEX " + index + " IF EXISTS" };
        case ADD_COLUMN:
            String column = "X" + Randomly.getNotCachedInteger(0, 1000);
            String[] typed = Randomly.fromOptions(new String[] { "INTEGER", "7", "= 7" },
                    new String[] { "VARCHAR(16)", "'dflt'", "= 'dflt'" },
                    new String[] { "VARCHAR(100)", "'a longer default kept outside the tuple'",
                            "= 'a longer default kept outside the tuple'" },
                    new String[] { "FLOAT", "1.5E0", "= 1.5E0" }, new String[] { "TIMESTAMP", null, "IS NULL" },
                    new String[] { "BIGINT", null, "IS NULL" });
            return new String[] {
                    "ALTER TABLE " + name + " ADD COLUMN " + column + " " + typed[0]
                            + (typed[1] == null ? "" : " DEFAULT " + typed[1]),
                    "ALTER TABLE " + name + " DROP COLUMN " + column, column, typed[2] };
        case WIDEN:
            VoltDBColumn c = Randomly.fromList(widenable);
            int size = c.getType().getSize() + Randomly.fromOptions(1, 16, 64, 200);
            return new String[] { "ALTER TABLE " + name + " ALTER COLUMN " + c.getName() + " VARCHAR(" + size + ")",
                    null };
        default:
            throw new AssertionError();
        }
    }

    // after ADD COLUMN, every row holds the DEFAULT (or NULL)
    private void checkNewColumn(VoltDBTable table, String[] change, String described) throws SQLException {
        String all = "SELECT COUNT(*) FROM " + table.getName();
        String matching = all + " WHERE " + change[2] + " " + change[3];
        List<String> expected = rows(all, 1);
        List<String> actual = rows(matching, 1);
        compare(all, matching, expected, actual, described);
    }

    // whether VoltDB applied the DDL; a refusal or an expected error skips the check
    private boolean ddl(String sql) {
        try (Statement st = state.getConnection().createStatement()) {
            st.execute(sql);
            return true;
        } catch (SQLException e) {
            return false;
        }
    }

    private List<String> rows(String sql, int columns) throws SQLException {
        lastQuery = sql;
        List<String> rows = VoltDBRows.strings(state, sql, errors, columns);
        java.util.Collections.sort(rows);
        return rows;
    }

    private void compare(String query, String after, List<String> expected, List<String> actual, String described) {
        if (expected.equals(actual)) {
            return;
        }
        // no reproducer: the mismatch depends on the order of the change and the queries, which a reduction of the
        // database's statements would not keep, so the message holds everything
        throw new AssertionError("schema change mismatch:" + System.lineSeparator() + described + "-- query: " + query
                + ';' + System.lineSeparator() + "-- rows before: " + expected.size() + ", " + after + ": "
                + actual.size() + System.lineSeparator() + "-- only before: " + VoltDBRows.difference(expected, actual)
                + System.lineSeparator() + "-- only after: " + VoltDBRows.difference(actual, expected));
    }

    @Override
    public Reproducer<VoltDBGlobalState> getLastReproducer() {
        return reproducer;
    }

    @Override
    public String getLastQueryString() {
        return lastQuery;
    }

}
