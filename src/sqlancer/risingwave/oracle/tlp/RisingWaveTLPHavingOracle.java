package sqlancer.risingwave.oracle.tlp;

import java.sql.SQLException;

import sqlancer.Reproducer;
import sqlancer.common.oracle.AbstractComparisonReproducer;
import sqlancer.common.oracle.TestOracleUtils;
import sqlancer.common.oracle.UnexpectedErrorReproducer;
import sqlancer.risingwave.RisingWaveGlobalState;
import sqlancer.risingwave.ast.RisingWaveExpression;
import sqlancer.risingwave.ast.RisingWaveSelect;
import sqlancer.risingwave.gen.RisingWaveQueryGenerator;
import sqlancer.risingwave.oracle.RisingWaveRows;

public final class RisingWaveTLPHavingOracle extends RisingWaveTLPBase {
    private RisingWaveSelect query;
    private String lastQuery;
    private Reproducer<RisingWaveGlobalState> reproducer;

    public RisingWaveTLPHavingOracle(RisingWaveGlobalState state) {
        super(state);
    }

    @Override
    protected RisingWaveExpression generatePredicate() {
        return gen.generateHavingPredicate(query);
    }

    @Override
    public void check() throws SQLException {
        reproducer = null;
        gen = new RisingWaveQueryGenerator(state)
                .setTablesAndColumns(TestOracleUtils.getRandomTableNonEmptyTables(state.getSchema()));
        query = gen.generateGroupedSelect();
        initializeTernaryPredicateVariants();
        String original = query.asString();
        lastQuery = original;
        query.setHavingClause(predicate);
        String first = query.asString();
        query.setHavingClause(negatedPredicate);
        String second = query.asString();
        query.setHavingClause(isNullPredicate);
        String third = query.asString();
        String union = first + " UNION ALL " + second + " UNION ALL " + third;
        String queryLines = "-- " + original + ";\n-- " + union + ";\n";

        RisingWaveRows baseline;
        RisingWaveRows partitions;
        try {
            baseline = execute(original, state);
            partitions = execute(union, state);
            baseline.requireSameMetadata(partitions);
        } catch (AssertionError unexpectedError) {
            reproducer = new UnexpectedErrorReproducer<>(replayState -> {
                RisingWaveRows originalRows = execute(original, replayState);
                originalRows.requireSameMetadata(execute(union, replayState));
            }, TestOracleUtils.getUnexpectedErrorMessage(unexpectedError), queryLines);
            throw unexpectedError;
        }
        reproducer = new AbstractComparisonReproducer<RisingWaveGlobalState, RisingWaveRows>() {
            @Override
            protected RisingWaveRows evaluateOriginal(RisingWaveGlobalState replayState) throws SQLException {
                return execute(original, replayState);
            }

            @Override
            protected RisingWaveRows evaluateTransformed(RisingWaveGlobalState replayState) throws SQLException {
                return execute(union, replayState);
            }

            @Override
            protected boolean sidesDiffer(RisingWaveRows originalRows, RisingWaveRows partitionRows,
                    RisingWaveGlobalState replayState) {
                return originalRows.sameMetadata(partitionRows) && !originalRows.same(partitionRows);
            }

            @Override
            protected String mismatchHeaderLine() {
                return "-- On the database set up by the statements above, the following queries mismatch:";
            }

            @Override
            protected void appendQueryLines(StringBuilder builder) {
                builder.append(queryLines);
            }
        };
        compare(baseline, partitions, original, union, state);
    }

    private RisingWaveRows execute(String sql, RisingWaveGlobalState replayState) throws SQLException {
        return RisingWaveRows.execute(sql, errors, replayState);
    }

    private static void compare(RisingWaveRows baseline, RisingWaveRows partitions, String original, String union,
            RisingWaveGlobalState replayState) {
        if (!baseline.same(partitions)) {
            String information = "-- " + original + ";\n-- " + union + ";\n";
            replayState.getState().getLocalState().log(information);
            throw new AssertionError(
                    "RisingWave complete-row results mismatch\n" + baseline + "\n" + partitions + "\n" + information);
        }
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
