package sqlancer.voltdb.oracle;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
import sqlancer.voltdb.gen.VoltDBExpressionGenerator;

/**
 * Checks UNION, INTERSECT and EXCEPT, each with and without ALL, against the same operation done in Java on the rows of
 * its operands. Each operand is {@code SELECT c1, ... FROM t [WHERE p]} over one table; the outputs at each position
 * share a type, as VoltDB requires, and an operand without a column of a position's type outputs a constant of it. The
 * query is {@code A op B}, or {@code (A op B) op C}, sometimes inside {@code SELECT * FROM (...) AS u}.
 *
 * <p>
 * In set operations NULLs are equal to each other, and exact numbers compare by value, so rows are compared by
 * {@link VoltDBRows#key}. VoltDB rejects a set operation over more than one partitioned table ("Statements are too
 * complex in set operation using multiple partitioned tables"), so at most one operand reads a partitioned table.
 */
public class VoltDBSetOperationOracle implements TestOracle<VoltDBGlobalState> {

    private enum Operator {
        UNION, UNION_ALL, INTERSECT, INTERSECT_ALL, EXCEPT, EXCEPT_ALL;

        String sql() {
            return name().replace('_', ' ');
        }

        // the result's multiplicity of a row that a holds m times and b n times
        int count(int m, int n) {
            switch (this) {
            case UNION:
                return m + n > 0 ? 1 : 0;
            case UNION_ALL:
                return m + n;
            case INTERSECT:
                return m > 0 && n > 0 ? 1 : 0;
            case INTERSECT_ALL:
                return Math.min(m, n);
            case EXCEPT:
                return m > 0 && n == 0 ? 1 : 0;
            case EXCEPT_ALL:
                return Math.max(m - n, 0);
            default:
                throw new AssertionError(this);
            }
        }
    }

    private final VoltDBGlobalState state;
    private final ExpectedErrors errors;
    private String lastQuery;
    private Reproducer<VoltDBGlobalState> reproducer;

    // VoltDB combines a set operation's columns only when they share a type: INTEGER with BIGINT, or TINYINT with
    // SMALLINT, is "Incompatible data types in UNION" (an integer literal is BIGINT). VARCHAR and VARBINARY of
    // different sizes combine.
    private static boolean usable(VoltDBDataType type) {
        return type != VoltDBDataType.BOOLEAN;
    }

    // a constant of the type, for an operand without a column of it; null for DATE (ENG-30560: a DATE constant makes
    // the statement's DECIMAL literals FLOAT)
    private static String constant(VoltDBDataType type, Randomly r) {
        switch (type) {
        case TINYINT:
        case SMALLINT:
        case INTEGER:
        case BIGINT:
        case DECIMAL:
            return "CAST(" + r.getInteger(-3, 4) + " AS " + type + ")";
        case FLOAT:
            return Randomly.fromOptions("1.5E0", "0.0E0", "(-2.25E0)");
        // ENG-30605: string literals at one position are padded to the longest ('a' UNION ALL 'abc' gives 'a '),
        // so they all have one character, and VARBINARY literals one byte
        case VARCHAR:
            return Randomly.fromOptions("'a'", "'b'");
        case VARBINARY:
            return Randomly.fromOptions("x'01'", "x'02'");
        case TIMESTAMP:
            return "CAST('2000-01-01 00:00:00.000000' AS TIMESTAMP)";
        default:
            return null;
        }
    }

    public VoltDBSetOperationOracle(VoltDBGlobalState state, ExpectedErrors errors) {
        this.state = state;
        this.errors = errors;
    }

    @Override
    public void check() throws Exception {
        reproducer = null;
        List<VoltDBTable> tables = candidateTables();
        int nrOperands = Randomly.getBoolean() ? 2 : 3;
        List<VoltDBTable> operandTables = new ArrayList<>();
        boolean partitioned = false;
        for (int i = 0; i < nrOperands; i++) {
            VoltDBTable t = Randomly.fromList(tables);
            if (t.isPartitioned()) {
                if (partitioned) {
                    t = Randomly.fromList(tables.stream().filter(x -> !x.isPartitioned()).collect(Collectors.toList()));
                }
                partitioned = true;
            }
            operandTables.add(t);
        }
        // the outputs' types, from the first operand's columns
        List<VoltDBColumn> firstColumns = operandTables.get(0).getColumns().stream()
                .filter(c -> usable(c.getType().getType())).collect(Collectors.toList());
        if (firstColumns.isEmpty()) {
            throw new IgnoreMeException();
        }
        List<VoltDBColumn> picked = Randomly.nonEmptySubset(firstColumns);
        List<VoltDBDataType> classes = picked.subList(0, Math.min(3, picked.size())).stream()
                .map(c -> c.getType().getType()).collect(Collectors.toList());

        List<String> operands = new ArrayList<>();
        for (VoltDBTable t : operandTables) {
            operands.add(operand(t, classes));
        }
        Operator first = Randomly.fromOptions(Operator.values());
        Operator second = nrOperands == 3 ? Randomly.fromOptions(Operator.values()) : null;
        String setOperation = nrOperands == 2 ? operands.get(0) + " " + first.sql() + " " + operands.get(1)
                : "(" + operands.get(0) + " " + first.sql() + " " + operands.get(1) + ") " + second.sql() + " "
                        + operands.get(2);
        // Unless --avoid-known-bugs is false, these set operations are not wrapped in a derived table:
        // - ENG-30604: one with an operand pinned to one partition returns the replicated operands' rows once per
        // partition, so one that reads a partitioned table;
        // - ENG-30607: the derived table's VARCHAR column takes the first operand's size when that is 16 characters or
        // more, and a longer value from another operand fails, so one with a VARCHAR or VARBINARY output.
        boolean avoid = state.getDbmsSpecificOptions().avoidKnownBugs;
        boolean strings = classes.stream().anyMatch(c -> c == VoltDBDataType.VARCHAR || c == VoltDBDataType.VARBINARY);
        boolean wrap = Randomly.getBoolean() && !(avoid && (partitioned || strings));
        String query = wrap ? "SELECT * FROM (" + setOperation + ") AS u" : setOperation;
        lastQuery = query;
        int columns = classes.size();

        StringBuilder described = new StringBuilder();
        for (String o : operands) {
            described.append("-- operand: ").append(o).append(';').append(System.lineSeparator());
        }
        described.append("-- set operation: ").append(query).append(';').append(System.lineSeparator());
        VoltDBRecheckReproducer.Check check = s -> compare(s, operands, first, second, query, columns);
        String problem;
        try {
            problem = check.run(state);
        } catch (AssertionError unexpectedError) {
            reproducer = new UnexpectedErrorReproducer<>(check::run,
                    TestOracleUtils.getUnexpectedErrorMessage(unexpectedError), described.toString());
            throw unexpectedError;
        }
        if (problem != null) {
            reproducer = new VoltDBRecheckReproducer(check, described.toString(), problem);
            throw new AssertionError("set operation mismatch: " + problem);
        }
    }

    // the database's tables that queries may read: those without a materialized view while --avoid-known-bugs is
    // true (ENG-30582)
    private List<VoltDBTable> candidateTables() {
        List<VoltDBTable> tables = new ArrayList<>(state.getSchema().getDatabaseTables());
        if (state.getDbmsSpecificOptions().avoidKnownBugs) {
            Set<String> viewed = state.getViews().values().stream().map(v -> v[0]).collect(Collectors.toSet());
            tables.removeIf(t -> viewed.contains(t.getName()));
        }
        if (tables.isEmpty() || tables.stream().allMatch(VoltDBTable::isPartitioned)) {
            throw new IgnoreMeException();
        }
        return tables;
    }

    // SELECT <a column or constant of each type> FROM t [WHERE p]
    private String operand(VoltDBTable t, List<VoltDBDataType> classes) {
        List<String> outputs = new ArrayList<>();
        for (VoltDBDataType c : classes) {
            List<VoltDBColumn> matching = t.getColumns().stream().filter(col -> col.getType().getType() == c)
                    .collect(Collectors.toList());
            if (!matching.isEmpty() && !Randomly.getBooleanWithRatherLowProbability()) {
                outputs.add(Randomly.fromList(matching).getFullQualifiedName());
            } else {
                String constant = constant(c, state.getRandomly());
                if (constant == null) {
                    throw new IgnoreMeException();
                }
                outputs.add(constant);
            }
        }
        StringBuilder sb = new StringBuilder("SELECT ").append(String.join(", ", outputs)).append(" FROM ")
                .append(t.getName());
        if (Randomly.getBoolean()) {
            VoltDBExpressionGenerator gen = new VoltDBExpressionGenerator(state);
            gen.setTablesAndColumns(new AbstractTables<>(List.of(t)));
            sb.append(" WHERE ").append(VoltDBToStringVisitor.asString(gen.generateBooleanExpression()));
        }
        return sb.toString();
    }

    // null when VoltDB's result is the operation done in Java on the operands' rows
    private String compare(VoltDBGlobalState s, List<String> operands, Operator first, Operator second, String query,
            int columns) throws SQLException {
        List<Map<String, Integer>> sides = new ArrayList<>();
        for (String o : operands) {
            sides.add(counts(VoltDBRows.values(s, o, errors, columns)));
        }
        Map<String, Integer> expected = apply(first, sides.get(0), sides.get(1));
        if (second != null) {
            expected = apply(second, expected, sides.get(2));
        }
        Map<String, Integer> actual = counts(VoltDBRows.values(s, query, errors, columns));
        if (expected.equals(actual)) {
            return null;
        }
        return "expected " + total(expected) + " rows, got " + total(actual) + System.lineSeparator()
                + "missing (row=count): " + difference(expected, actual) + System.lineSeparator()
                + "unexpected (row=count): " + difference(actual, expected);
    }

    private static Map<String, Integer> counts(List<List<Object>> rows) {
        Map<String, Integer> counts = new LinkedHashMap<>();
        for (List<Object> row : rows) {
            counts.merge(VoltDBRows.key(row), 1, Integer::sum);
        }
        return counts;
    }

    private static Map<String, Integer> apply(Operator op, Map<String, Integer> a, Map<String, Integer> b) {
        Map<String, Integer> result = new LinkedHashMap<>();
        List<String> keys = new ArrayList<>(a.keySet());
        b.keySet().stream().filter(k -> !a.containsKey(k)).forEach(keys::add);
        for (String k : keys) {
            int n = op.count(a.getOrDefault(k, 0), b.getOrDefault(k, 0));
            if (n > 0) {
                result.put(k, n);
            }
        }
        return result;
    }

    private static int total(Map<String, Integer> counts) {
        return counts.values().stream().mapToInt(Integer::intValue).sum();
    }

    // the rows that a holds more often than b, with how many more times
    private static List<String> difference(Map<String, Integer> a, Map<String, Integer> b) {
        List<String> rest = new ArrayList<>();
        for (Map.Entry<String, Integer> e : a.entrySet()) {
            int extra = e.getValue() - b.getOrDefault(e.getKey(), 0);
            if (extra > 0) {
                rest.add(e.getKey() + "=" + extra);
            }
        }
        Collections.sort(rest);
        return rest;
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
