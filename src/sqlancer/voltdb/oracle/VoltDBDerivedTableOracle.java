package sqlancer.voltdb.oracle;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.Reproducer;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.oracle.TestOracleUtils;
import sqlancer.common.oracle.UnexpectedErrorReproducer;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.schema.AbstractTables;
import sqlancer.voltdb.VoltDBProvider.VoltDBGlobalState;
import sqlancer.voltdb.VoltDBSchema.VoltDBColumn;
import sqlancer.voltdb.VoltDBSchema.VoltDBDataType;
import sqlancer.voltdb.VoltDBSchema.VoltDBTable;
import sqlancer.voltdb.VoltDBToStringVisitor;
import sqlancer.voltdb.ast.VoltDBAlias;
import sqlancer.voltdb.ast.VoltDBColumnReference;
import sqlancer.voltdb.ast.VoltDBExpression;
import sqlancer.voltdb.ast.VoltDBSelect;
import sqlancer.voltdb.gen.VoltDBExpressionGenerator;

/**
 * Checks derived tables and common table expressions (WITH) against the queries they stand for. Three modes:
 * <ul>
 * <li>{@code WRAP}: a query {@code SELECT f FROM ... WHERE p} returns the rows of
 * {@code SELECT * FROM (that) AS d}.</li>
 * <li>{@code PUSH}: it returns the rows of the query that selects every FROM column under an alias in a derived table,
 * {@code SELECT d.f' FROM (SELECT t.c AS A0, ... FROM ...) AS d WHERE p'}, where f' and p' read the aliases, so that
 * VoltDB pushes p back down, or the same written {@code WITH d AS (...) SELECT ...}. VoltDB allows a common table only
 * over replicated tables in a multi-partition query, so the WITH form reads replicated tables alone.</li>
 * <li>{@code RECURSIVE}: {@code WITH RECURSIVE rc (v) AS (SELECT t.c FROM t WHERE p UNION ALL SELECT t.c FROM t, rc
 * WHERE t.c = rc.v + 1) SELECT v FROM rc} over an integer column of a replicated table returns the closure the oracle
 * computes in Java from the table's values: every start value, and from each value every row whose value is one
 * more.</li>
 * </ul>
 * Rows are compared as multisets, by {@link VoltDBRows#key}.
 */
public class VoltDBDerivedTableOracle implements TestOracle<VoltDBGlobalState> {

    private enum Mode {
        WRAP, PUSH, RECURSIVE
    }

    // a recursive query that would return more rows than this is not checked
    private static final int MAX_CLOSURE = 5000;

    private final VoltDBGlobalState state;
    private final VoltDBExpressionGenerator gen;
    private final ExpectedErrors errors;
    private String lastQuery;
    private Reproducer<VoltDBGlobalState> reproducer;

    public VoltDBDerivedTableOracle(VoltDBGlobalState state, ExpectedErrors errors) {
        this.state = state;
        this.gen = new VoltDBExpressionGenerator(state);
        this.errors = errors;
    }

    @Override
    public void check() throws Exception {
        reproducer = null;
        // ENG-30608: a node crashed twice, beside the PROCEDURE oracle's catalog updates, in an endless recursion of
        // the
        // execution engine's common-table code while running this oracle's WITH RECURSIVE query, so unless
        // --avoid-known-bugs is false the RECURSIVE mode is off
        Mode mode = state.getDbmsSpecificOptions().avoidKnownBugs ? Randomly.fromOptions(Mode.WRAP, Mode.PUSH)
                : Randomly.fromOptions(Mode.values());
        if (mode == Mode.RECURSIVE) {
            checkRecursive();
            return;
        }
        gen.setTablesAndColumns(TestOracleUtils.getRandomTableNonEmptyTables(state.getSchema()));
        VoltDBSelect select = gen.generateSelect();
        select.setFromList(gen.getTableRefs());
        select.setJoinClauses(gen.getRandomJoinClauses());
        List<VoltDBExpression> fetch = gen.generateFetchColumns();
        select.setFetchColumns(fetch);
        VoltDBExpression where = gen.generateBooleanExpression();
        select.setWhereClause(where);
        String original = select.asString();
        int columns = fetch.size();

        String other;
        if (mode == Mode.WRAP) {
            other = "SELECT * FROM (" + original + ") AS d";
        } else {
            // every FROM column under its own alias, and the fetch list and WHERE clause rewritten to read them
            List<VoltDBColumn> all = gen.getColumns();
            List<VoltDBExpression> inner = new ArrayList<>();
            Map<String, String> renames = new HashMap<>();
            for (int i = 0; i < all.size(); i++) {
                inner.add(new VoltDBAlias(new VoltDBColumnReference(all.get(i)), "A" + i));
                renames.put(all.get(i).getFullQualifiedName(), "D.A" + i);
            }
            select.setFetchColumns(inner);
            select.setWhereClause(null);
            String innerQuery = select.asString();
            String outerFetch = rename(
                    fetch.stream().map(VoltDBToStringVisitor::asString).collect(Collectors.joining(", ")), renames);
            String outerWhere = rename(VoltDBToStringVisitor.asString(where), renames);
            if (gen.isReplicatedOnly() && Randomly.getBoolean()) {
                other = "WITH D AS (" + innerQuery + ") SELECT " + outerFetch + " FROM D WHERE " + outerWhere;
            } else {
                other = "SELECT " + outerFetch + " FROM (" + innerQuery + ") AS D WHERE " + outerWhere;
            }
        }
        lastQuery = other;
        String described = "-- query: " + original + ';' + System.lineSeparator() + "-- with a derived table: " + other
                + ';' + System.lineSeparator();
        run(described, s -> compare(s, original, other, columns));
    }

    // replaces each column name by its alias, longest names first so that no name is replaced inside a longer one
    private static String rename(String sql, Map<String, String> renames) {
        String result = sql;
        List<String> names = new ArrayList<>(renames.keySet());
        names.sort(Comparator.comparingInt(String::length).reversed());
        for (String name : names) {
            result = result.replace(name, renames.get(name));
        }
        return result;
    }

    private void checkRecursive() throws Exception {
        List<VoltDBTable> candidates = state.getSchema().getDatabaseTables().stream().filter(
                t -> !t.isPartitioned() && t.getColumns().stream().anyMatch(VoltDBDerivedTableOracle::isInteger))
                .collect(Collectors.toList());
        if (candidates.isEmpty()) {
            throw new IgnoreMeException();
        }
        VoltDBTable t = Randomly.fromList(candidates);
        gen.setTablesAndColumns(new AbstractTables<>(List.of(t)));
        VoltDBColumn c = Randomly.fromList(
                t.getColumns().stream().filter(VoltDBDerivedTableOracle::isInteger).collect(Collectors.toList()));
        String col = c.getFullQualifiedName();
        String start = "SELECT " + col + " FROM " + t.getName() + " WHERE "
                + VoltDBToStringVisitor.asString(gen.generateBooleanExpression());
        String values = "SELECT " + col + " FROM " + t.getName();
        String query = "WITH RECURSIVE RC (V) AS (" + start + " UNION ALL SELECT " + col + " FROM " + t.getName()
                + ", RC WHERE " + col + " = RC.V + 1) SELECT V FROM RC";
        lastQuery = query;
        String described = "-- recursive: " + query + ';' + System.lineSeparator() + "-- start: " + start + ';'
                + System.lineSeparator() + "-- values: " + values + ';' + System.lineSeparator();
        run(described, s -> compareRecursive(s, start, values, query));
    }

    private static boolean isInteger(VoltDBColumn c) {
        VoltDBDataType type = c.getType().getType();
        return type == VoltDBDataType.TINYINT || type == VoltDBDataType.SMALLINT || type == VoltDBDataType.INTEGER
                || type == VoltDBDataType.BIGINT;
    }

    private void run(String described, VoltDBRecheckReproducer.Check check) throws Exception {
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
            throw new AssertionError("derived table mismatch: " + problem);
        }
    }

    // null when the two queries return the same rows
    private String compare(VoltDBGlobalState s, String a, String b, int columns) throws SQLException {
        List<String> x = keys(VoltDBRows.values(s, a, errors, columns));
        List<String> y = keys(VoltDBRows.values(s, b, errors, columns));
        if (x.equals(y)) {
            return null;
        }
        return "rows of the query: " + x.size() + ", with the derived table: " + y.size() + System.lineSeparator()
                + "only in the query: " + VoltDBRows.difference(x, y) + System.lineSeparator()
                + "only with the derived table: " + VoltDBRows.difference(y, x);
    }

    // null when the recursive query returns the closure of its start values
    private String compareRecursive(VoltDBGlobalState s, String start, String values, String query)
            throws SQLException {
        Map<Long, Integer> multiplicity = new HashMap<>();
        for (List<Object> row : VoltDBRows.values(s, values, errors, 1)) {
            if (row.get(0) != null) {
                multiplicity.merge(((Number) row.get(0)).longValue(), 1, Integer::sum);
            }
        }
        List<Long> frontier = new ArrayList<>();
        List<String> expected = new ArrayList<>();
        for (List<Object> row : VoltDBRows.values(s, start, errors, 1)) {
            if (row.get(0) == null) {
                expected.add("null"); // a row of the result, whose next step joins nothing
            } else {
                frontier.add(((Number) row.get(0)).longValue());
            }
        }
        while (!frontier.isEmpty()) {
            List<Long> next = new ArrayList<>();
            for (long v : frontier) {
                expected.add(Long.toString(v));
                if (expected.size() > MAX_CLOSURE) {
                    throw new IgnoreMeException();
                }
                for (int i = 0; i < multiplicity.getOrDefault(v + 1, 0); i++) {
                    next.add(v + 1);
                }
            }
            frontier = next;
        }
        Collections.sort(expected);
        List<String> actual = keys(VoltDBRows.values(s, query, errors, 1));
        if (expected.equals(actual)) {
            return null;
        }
        return "expected " + expected.size() + " rows, got " + actual.size() + System.lineSeparator() + "missing: "
                + VoltDBRows.difference(expected, actual) + System.lineSeparator() + "unexpected: "
                + VoltDBRows.difference(actual, expected);
    }

    private static List<String> keys(List<List<Object>> rows) {
        List<String> keys = new ArrayList<>();
        for (List<Object> row : rows) {
            keys.add(VoltDBRows.key(row));
        }
        Collections.sort(keys);
        return keys;
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
