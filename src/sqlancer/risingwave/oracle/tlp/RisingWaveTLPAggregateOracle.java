package sqlancer.risingwave.oracle.tlp;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import sqlancer.IgnoreMeException;
import sqlancer.Reproducer;
import sqlancer.common.oracle.AbstractComparisonReproducer;
import sqlancer.common.oracle.TestOracleUtils;
import sqlancer.common.oracle.UnexpectedErrorReproducer;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLancerResultSet;
import sqlancer.risingwave.RisingWaveGlobalState;
import sqlancer.risingwave.RisingWaveType;
import sqlancer.risingwave.ast.RisingWaveAggregateFunction;
import sqlancer.risingwave.ast.RisingWaveAlias;
import sqlancer.risingwave.ast.RisingWaveExpression;
import sqlancer.risingwave.ast.RisingWaveFunctionCall;
import sqlancer.risingwave.ast.RisingWaveOperation;
import sqlancer.risingwave.ast.RisingWaveOperation.Operator;
import sqlancer.risingwave.ast.RisingWaveSelect;
import sqlancer.risingwave.gen.RisingWaveQueryGenerator;

public final class RisingWaveTLPAggregateOracle extends RisingWaveTLPBase {
    private String lastQuery;
    private Reproducer<RisingWaveGlobalState> reproducer;

    public RisingWaveTLPAggregateOracle(RisingWaveGlobalState state) {
        super(state);
    }

    static List<String> queries(RisingWaveSelect input, List<RisingWaveExpression> predicates) {
        RisingWaveFunctionCall aggregate = (RisingWaveFunctionCall) input.getFetchColumns().get(0);
        RisingWaveAggregateFunction combiner = aggregate.aggregate == null ? null
                : aggregate.aggregate.getTlpCombiner();
        if (combiner == null || aggregate.getType().floatingPoint()
                || aggregate.arguments.stream()
                        .anyMatch(a -> a.getType().floatingPoint() || a.getType().array() || a.getType().struct())
                || aggregate.distinct || aggregate.filter != null || !aggregate.orderBy.isEmpty()
                || !input.getGroupByClause().isEmpty() || !input.getGroupingSets().isEmpty()
                || input.getHavingClause() != null) {
            throw new IllegalArgumentException("Decomposition requires one plain global aggregate");
        }
        List<String> arms = new ArrayList<>();
        for (RisingWaveExpression partition : predicates) {
            RisingWaveSelect arm = new RisingWaveSelect();
            arm.setFromList(input.getFromList());
            arm.setJoinClauses(input.getJoinClauses());
            arm.setFetchColumns(List.of(new RisingWaveAlias(aggregate, "a")));
            arm.setWhereClause(input.getWhereClause() == null ? partition
                    : new RisingWaveOperation(Operator.AND, RisingWaveType.BOOLEAN, input.getWhereClause(), partition));
            arms.add(arm.asString());
        }
        String outer = combiner.getName();
        return List.of(input.asString(), "SELECT " + outer + "(\"rw_partial\".\"a\") FROM ("
                + String.join(" UNION ALL ", arms) + ") AS \"rw_partial\"");
    }

    @Override
    public void check() throws SQLException {
        reproducer = null;
        gen = new RisingWaveQueryGenerator(state)
                .setTablesAndColumns(TestOracleUtils.getRandomTableNonEmptyTables(state.getSchema()));
        RisingWaveSelect input = gen.generateAggregateSelect();
        initializeTernaryPredicateVariants();
        RisingWaveType type = input.getFetchColumns().get(0).getType();
        List<String> queries = queries(input, List.of(predicate, negatedPredicate, isNullPredicate));
        String original = queries.get(0);
        String transformed = queries.get(1);
        String information = "-- " + original + ";\n-- " + transformed + ";\n";
        lastQuery = original;
        List<String> left;
        List<String> right;
        try {
            left = execute(original, state);
            right = execute(transformed, state);
        } catch (AssertionError failure) {
            reproducer = new UnexpectedErrorReproducer<>(candidate -> {
                execute(original, candidate);
                execute(transformed, candidate);
            }, TestOracleUtils.getUnexpectedErrorMessage(failure), information);
            throw failure;
        }
        reproducer = new AbstractComparisonReproducer<RisingWaveGlobalState, List<String>>() {
            @Override
            protected List<String> evaluateOriginal(RisingWaveGlobalState candidate) throws SQLException {
                return execute(original, candidate);
            }

            @Override
            protected List<String> evaluateTransformed(RisingWaveGlobalState candidate) throws SQLException {
                return execute(transformed, candidate);
            }

            @Override
            protected boolean sidesDiffer(List<String> baseline, List<String> partitions,
                    RisingWaveGlobalState candidate) {
                return !equal(baseline, partitions, type);
            }

            @Override
            protected String mismatchHeaderLine() {
                return "-- On the database set up by the statements above, the following aggregates mismatch:";
            }

            @Override
            protected void appendQueryLines(StringBuilder builder) {
                builder.append(information);
            }
        };
        if (!equal(left, right, type)) {
            state.getState().getLocalState().log(information);
            throw new AssertionError("Aggregate results mismatch: " + left + " versus " + right + "\n" + information);
        }
    }

    static boolean equal(List<String> left, List<String> right, RisingWaveType type) {
        // SUM of partial COUNTs or integer SUMs widens to DECIMAL. Compare exact values, never doubles.
        if (left.size() != 1 || right.size() != 1) {
            return false;
        }
        String first = left.get(0);
        String second = right.get(0);
        if (first == null || second == null) {
            return first == second;
        }
        return type.numeric() ? new BigDecimal(first).compareTo(new BigDecimal(second)) == 0
                : Objects.equals(first, second);
    }

    private List<String> execute(String sql, RisingWaveGlobalState candidate) throws SQLException {
        if (candidate.getOptions().logEachSelect()) {
            candidate.getLogger().writeCurrent(sql);
            try {
                candidate.getLogger().getCurrentFileWriter().flush();
            } catch (IOException failure) {
                throw new AssertionError("Cannot log aggregate query", failure);
            }
        }
        List<String> rows = new ArrayList<>();
        try (SQLancerResultSet result = new SQLQueryAdapter(sql, errors).executeAndGet(candidate)) {
            if (result == null) {
                throw new IgnoreMeException();
            }
            while (result.next()) {
                rows.add(result.getString(1));
            }
        } catch (Exception failure) {
            if (failure instanceof IgnoreMeException) {
                throw (IgnoreMeException) failure;
            }
            for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
                if (cause.getMessage() != null && errors.errorIsExpected(cause.getMessage())) {
                    throw new IgnoreMeException();
                }
            }
            throw new AssertionError(sql, failure);
        }
        return rows;
    }

    @Override
    public String getLastQueryString() {
        return lastQuery;
    }

    @Override
    public Reproducer<RisingWaveGlobalState> getLastReproducer() {
        return reproducer;
    }
}
