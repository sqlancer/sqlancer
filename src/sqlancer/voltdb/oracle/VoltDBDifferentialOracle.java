package sqlancer.voltdb.oracle;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.Reproducer;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.oracle.TestOracleUtils;
import sqlancer.common.oracle.UnexpectedErrorReproducer;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.voltdb.VoltDBConnection;
import sqlancer.voltdb.VoltDBProvider.VoltDBGlobalState;
import sqlancer.voltdb.VoltDBSchema.VoltDBDataType;
import sqlancer.voltdb.ast.VoltDBAlias;
import sqlancer.voltdb.ast.VoltDBColumnReference;
import sqlancer.voltdb.ast.VoltDBExpression;
import sqlancer.voltdb.ast.VoltDBSelect;
import sqlancer.voltdb.gen.VoltDBExpressionGenerator;
import sqlancer.voltdb.gen.VoltDBExpressionGenerator.Kind;

/**
 * Differential testing against another VoltDB version. With {@code --mirror}, every statement that changes the database
 * also runs on a second cluster, normally the last release (see VoltDBConnection), so the two hold the same rows. DIFF
 * runs the same generated query on both and compares their rows as multisets: a SELECT of columns and expressions with
 * a WHERE clause, or a GROUP BY query with aggregates and maybe WHERE and HAVING clauses, over the FROM clauses and
 * joins the other oracles use.
 *
 * <p>
 * FLOATs are compared to 12 significant digits, as a SUM of FLOATs may add them in another order on each cluster. A
 * query with an expected error on either cluster is skipped, as is every check once a change succeeded on one cluster
 * and failed on the other (the databases may then differ). A difference is a change between the versions: a bug fixed,
 * a bug introduced, or a deliberate change in behavior. A failure hands SQLancer a reproducer, so {@code --use-reducer}
 * can reduce it; the reduction replays the statements on both clusters.
 */
public class VoltDBDifferentialOracle implements TestOracle<VoltDBGlobalState> {

    private final VoltDBGlobalState state;
    private final ExpectedErrors errors;
    private String lastQuery;
    private Reproducer<VoltDBGlobalState> reproducer;

    public VoltDBDifferentialOracle(VoltDBGlobalState state, ExpectedErrors errors) {
        this.state = state;
        this.errors = errors;
    }

    @Override
    public void check() throws Exception {
        reproducer = null;
        VoltDBConnection connection = (VoltDBConnection) state.getConnection();
        if (connection.getMirror() == null || connection.isDiverged()) {
            throw new IgnoreMeException();
        }
        VoltDBExpressionGenerator gen = new VoltDBExpressionGenerator(state);
        boolean grouped = Randomly.getBoolean();
        // ENG-30531 (see VoltDBTLPOracle): no FULL JOIN in a GROUP BY query
        gen.setNoFullJoins(grouped);
        gen.setTablesAndColumns(TestOracleUtils.getRandomTableNonEmptyTables(state.getSchema()));
        VoltDBSelect select = gen.generateSelect();
        select.setFromList(gen.getTableRefs());
        select.setJoinClauses(gen.getRandomJoinClauses());
        if (grouped) {
            List<VoltDBExpression> grouping = new ArrayList<>(gen.generateGroupingColumns());
            if (state.getDbmsSpecificOptions().avoidKnownBugs) {
                // ENG-30255 (fixed after 16.0.0): grouping by a DECIMAL that is NULL aborts a 16.0.0 server
                // (NValue::hashCombine throws in AggregateHashExecutor), which the mirror normally runs, so DIFF does
                // not group by DECIMAL columns
                grouping.removeIf(e -> e instanceof VoltDBColumnReference
                        && ((VoltDBColumnReference) e).getColumn().getType().getType() == VoltDBDataType.DECIMAL);
                if (grouping.isEmpty()) {
                    throw new IgnoreMeException();
                }
            }
            List<VoltDBExpression> fetched = new ArrayList<>(Randomly.subset(grouping));
            if (fetched.isEmpty() || Randomly.getBoolean()) {
                fetched.addAll(gen.generateAggregates());
            }
            select.setFetchColumns(gen.aliased(fetched));
            select.setGroupByExpressions(grouping);
            if (Randomly.getBoolean()) {
                select.setWhereClause(gen.generateBooleanExpression());
            }
            if (Randomly.getBoolean()) {
                select.setHavingClause(gen.generateHavingPredicate(grouping));
            }
        } else {
            List<VoltDBExpression> fetched = new ArrayList<>(gen.generateFetchColumns());
            for (int i = Randomly.smallNumber(); i > 0; i--) {
                fetched.add(
                        new VoltDBAlias(gen.generateExpressionInDomain(Randomly.fromOptions(Kind.values())), "e" + i));
            }
            select.setFetchColumns(fetched);
            select.setWhereClause(gen.generateBooleanExpression());
        }
        String query = select.asString();
        int columns = select.getFetchColumns().size();
        lastQuery = query;
        String described = "-- query on both clusters: " + query + ';' + System.lineSeparator();
        VoltDBRecheckReproducer.Check check = s -> compare(s, query, columns);
        String problem;
        try {
            problem = check.run(state);
        } catch (AssertionError unexpectedError) {
            reproducer = new UnexpectedErrorReproducer<>(check::run,
                    TestOracleUtils.getUnexpectedErrorMessage(unexpectedError), described);
            throw unexpectedError;
        }
        if (problem != null) {
            reproducer = new VoltDBRecheckReproducer(check, described, problem);
            throw new AssertionError("difference between the versions: " + problem);
        }
    }

    // null when both clusters return the same rows
    private String compare(VoltDBGlobalState s, String query, int columns) throws SQLException {
        VoltDBConnection connection = (VoltDBConnection) s.getConnection();
        if (connection.getMirror() == null || connection.isDiverged()) {
            throw new IgnoreMeException();
        }
        List<String> here = rows(connection, query, columns);
        List<String> there = rows(connection.getMirror(), query, columns);
        if (here.equals(there)) {
            return null;
        }
        return "rows on this version: " + here.size() + ", on the mirror: " + there.size() + System.lineSeparator()
                + "only on this version: " + VoltDBRows.difference(here, there) + System.lineSeparator()
                + "only on the mirror: " + VoltDBRows.difference(there, here);
    }

    // the query's rows, each its values joined with " | ", sorted; an expected error on either cluster skips the check
    private List<String> rows(VoltDBConnection connection, String query, int columns) throws SQLException {
        List<String> rows = new ArrayList<>();
        try (Statement st = connection.createStatement(); ResultSet rs = st.executeQuery(query)) {
            while (rs.next()) {
                List<String> row = new ArrayList<>();
                for (int i = 1; i <= columns; i++) {
                    row.add(value(rs.getObject(i)));
                }
                rows.add(String.join(" | ", row));
            }
        } catch (SQLException e) {
            if (errors.errorIsExpected(e.getMessage())) {
                throw new IgnoreMeException();
            }
            throw new AssertionError(query, e);
        }
        Collections.sort(rows);
        return rows;
    }

    private static String value(Object v) {
        if (v instanceof Double) {
            double d = (Double) v;
            return d == 0 ? "0" : String.format("%.12g", d);
        }
        if (v instanceof BigDecimal) {
            BigDecimal d = ((BigDecimal) v).stripTrailingZeros();
            return d.signum() == 0 ? "0" : d.toPlainString();
        }
        if (v instanceof byte[]) {
            StringBuilder sb = new StringBuilder("x'");
            for (byte b : (byte[]) v) {
                sb.append(String.format("%02X", b));
            }
            return sb.append('\'').toString();
        }
        return String.valueOf(v);
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
