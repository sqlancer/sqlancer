package sqlancer.voltdb.oracle;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.Reproducer;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.oracle.TestOracleUtils;
import sqlancer.common.oracle.UnexpectedErrorReproducer;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.voltdb.VoltDBProvider.VoltDBGlobalState;
import sqlancer.voltdb.VoltDBSchema.VoltDBTable;
import sqlancer.voltdb.gen.VoltDBDeleteGenerator;
import sqlancer.voltdb.gen.VoltDBInsertGenerator;
import sqlancer.voltdb.gen.VoltDBUpdateGenerator;

/**
 * Materialized views. A view must hold the rows its defining query returns when run on the table now: VoltDB keeps the
 * view up to date as the table changes, rather than running the query again. The defining query is run twice: with
 * {@code ORDER BY 1}, which VoltDB's view rewriter leaves alone, so it is computed from the table; and as written,
 * which the rewriter may answer from the view. All three must have the same rows. The views are made with the database
 * (see VoltDBViewGenerator), whose inserts, updates and deletes already change the tables under them. Half the time the
 * oracle first runs one more INSERT, UPDATE or DELETE on the view's table, so that the check follows a change made
 * right before it.
 *
 * <p>
 * VoltDB combines a view's per-partition rows when it is read, so a view on a partitioned table also has one row per
 * group. Rows are compared whole, as multisets, as TLP compares them. A failure reports both queries, and
 * {@code --use-reducer} works.
 */
public class VoltDBViewOracle implements TestOracle<VoltDBGlobalState> {

    private final VoltDBGlobalState state;
    private final ExpectedErrors errors;
    private String lastQuery;
    private Reproducer<VoltDBGlobalState> reproducer;

    public VoltDBViewOracle(VoltDBGlobalState state, ExpectedErrors errors) {
        this.state = state;
        this.errors = errors;
    }

    @Override
    public void check() throws Exception {
        reproducer = null;
        Map<String, String[]> views = state.getViews();
        if (views.isEmpty()) {
            throw new IgnoreMeException();
        }
        String view = Randomly.fromList(new ArrayList<>(views.keySet()));
        String[] definition = views.get(view);
        VoltDBTable table = state.getSchema().getDatabaseTables().stream()
                .filter(t -> t.getName().equals(definition[0])).findFirst().orElseThrow(IgnoreMeException::new);
        if (Randomly.getBoolean()) {
            SQLQueryAdapter change;
            switch (Randomly.fromOptions(0, 1, 2)) {
            case 0:
                change = VoltDBInsertGenerator.getQuery(state, table);
                break;
            case 1:
                change = VoltDBUpdateGenerator.getQuery(state, table);
                break;
            default:
                change = VoltDBDeleteGenerator.getQuery(state, table);
                break;
            }
            state.executeStatement(change);
        }
        Queries queries = new Queries("SELECT * FROM " + view, definition[1] + " ORDER BY 1", definition[1]);
        lastQuery = queries.view;

        Result result;
        try {
            result = queries.run(state);
        } catch (AssertionError unexpectedError) {
            reproducer = new UnexpectedErrorReproducer<>(queries::run,
                    TestOracleUtils.getUnexpectedErrorMessage(unexpectedError), queries.describe());
            throw unexpectedError;
        }
        if (result.differs()) {
            reproducer = new MismatchReproducer(queries, result);
            throw new AssertionError("view mismatch: " + result.view.size() + " rows in the view, "
                    + result.query.size() + " from its query, " + result.rewritten.size() + " from it as written");
        }
    }

    // The view's own rows, its defining query computed from the table, and the query as written, and how to run them
    private final class Queries {
        final String view;
        final String query;
        final String rewritable;

        Queries(String view, String query, String rewritable) {
            this.view = view;
            this.query = query;
            this.rewritable = rewritable;
        }

        // An expected error in either query throws IgnoreMeException, such as for a view whose CREATE VIEW failed;
        // an unexpected one throws AssertionError
        Result run(VoltDBGlobalState s) throws SQLException {
            return new Result(rows(s, view), rows(s, query), rows(s, rewritable));
        }

        // each row as its values joined with " | ", as VoltDBTLPOracle writes them
        private List<String> rows(VoltDBGlobalState s, String sql) throws SQLException {
            List<String> rows = new ArrayList<>();
            try (Statement st = s.getConnection().createStatement(); ResultSet rs = st.executeQuery(sql)) {
                int columns = rs.getMetaData().getColumnCount();
                while (rs.next()) {
                    List<String> row = new ArrayList<>();
                    for (int i = 1; i <= columns; i++) {
                        Object value = rs.getObject(i);
                        row.add(value instanceof Double ? String.format("%.12g", (Double) value + 0.0)
                                : String.valueOf(rs.getString(i)));
                    }
                    rows.add(String.join(" | ", row));
                }
            } catch (SQLException e) {
                if (errors.errorIsExpected(e.getMessage())) {
                    throw new IgnoreMeException();
                }
                throw new AssertionError(sql, e);
            }
            return rows;
        }

        // the queries as commented lines, each ending in a line separator
        String describe() {
            return "-- view: " + view + ";" + System.lineSeparator() + "-- its query: " + query + ";"
                    + System.lineSeparator() + "-- as written: " + rewritable + ";" + System.lineSeparator();
        }
    }

    // The rows of the view, of its query and of its query as written, each sorted
    private static final class Result {
        final List<String> view;
        final List<String> query;
        final List<String> rewritten;

        Result(List<String> view, List<String> query, List<String> rewritten) {
            this.view = new ArrayList<>(view);
            this.query = new ArrayList<>(query);
            this.rewritten = new ArrayList<>(rewritten);
            Collections.sort(this.view);
            Collections.sort(this.query);
            Collections.sort(this.rewritten);
        }

        boolean differs() {
            return !view.equals(query) || !rewritten.equals(query);
        }

        // the rows of a that b lacks, counting duplicates
        static List<String> difference(List<String> a, List<String> b) {
            List<String> rest = new ArrayList<>(a);
            for (String row : b) {
                rest.remove(row);
            }
            return rest;
        }
    }

    // Re-runs both queries and reports the bug as still present while they still differ, with the rows of the last run
    // that did, as VoltDBTLPOracle's reproducer does
    private static final class MismatchReproducer implements Reproducer<VoltDBGlobalState> {
        private final Queries queries;
        private Result result;

        MismatchReproducer(Queries queries, Result result) {
            this.queries = queries;
            this.result = result;
        }

        @Override
        public boolean bugStillTriggers(VoltDBGlobalState globalState) {
            try {
                Result rerun = queries.run(globalState);
                if (rerun.differs()) {
                    result = rerun;
                    return true;
                }
                return false;
            } catch (AssertionError | SQLException | RuntimeException e) {
                // an error, expected or not, is an artifact of the reduction, such as a removed view
                return false;
            }
        }

        @Override
        public String getBugInformation() {
            return "-- On the database set up by the statements above, the rows of these queries mismatch:"
                    + System.lineSeparator() + queries.describe() + "-- view rows: " + result.view.size()
                    + ", query rows: " + result.query.size() + ", as written: " + result.rewritten.size()
                    + System.lineSeparator() + "-- rows only in the view: "
                    + Result.difference(result.view, result.query) + System.lineSeparator()
                    + "-- rows only from its query: " + Result.difference(result.query, result.view)
                    + System.lineSeparator() + "-- rows only as written: "
                    + Result.difference(result.rewritten, result.query) + System.lineSeparator()
                    + "-- rows missing as written: " + Result.difference(result.query, result.rewritten)
                    + System.lineSeparator();
        }
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
