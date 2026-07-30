package sqlancer.common.oracle;

import java.sql.SQLException;

import sqlancer.Reproducer;
import sqlancer.SQLGlobalState;

/**
 * Reproducer for a bug that is an unexpected DBMS error, rather than a violation of oracle logic. When a statement run
 * by the oracle raises an error the oracle did not expect, the bug is that error. Reduction re-runs the statement
 * execution against the reduced database and reports whether the same error still fires. The oracle supplies how to
 * re-run its execution as an {@link Execution} functional interface.
 *
 * <p>
 * For an oracle to use this reproducer on its unexpected errors, they must surface as {@link AssertionError}s, even
 * though they likely originated as {@link SQLException}s. This is because
 * {@link UnexpectedErrorReproducer#bugStillTriggers} treats a {@link SQLException} as a replay failure that means "bug
 * does not trigger" (e.g. dropped connection, removed table).
 *
 * @param <G>
 *            the DBMS-specific global state class
 */
public final class UnexpectedErrorReproducer<G extends SQLGlobalState<?, ?>> implements Reproducer<G> {

    private final Execution<G> execution;
    private final String expectedErrorMessage;
    private final String queryLines;

    @FunctionalInterface
    public interface Execution<G> {
        /**
         * Re-runs the oracle's execution against the reduced database. The bug is treated as still present if the run
         * continues to raise an {@link AssertionError} with the same message.
         *
         * @param globalState
         *            the state whose connection points at the reduced database
         *
         * @throws SQLException
         *             if a DBMS interaction fails for a reason other than the recorded bug (e.g. a connection or setup
         *             failure during replay), which counts as the bug no longer triggering
         */
        void execute(G globalState) throws SQLException;
    }

    /**
     * @param execution
     *            re-runs the oracle's execution against the reduced database
     * @param expectedErrorMessage
     *            the message of the error the original bug was. The bug is treated as still present if the
     *            {@link Execution} continues to raise an {@link AssertionError} with the same message.
     * @param queryLines
     *            the failing queries as commented lines (each ending in a line separator), for the reduced test case
     */
    public UnexpectedErrorReproducer(Execution<G> execution, String expectedErrorMessage, String queryLines) {
        this.execution = execution;
        this.expectedErrorMessage = expectedErrorMessage;
        this.queryLines = queryLines;
    }

    @Override
    public boolean bugStillTriggers(G globalState) {
        try {
            execution.execute(globalState);
        } catch (AssertionError unexpectedError) {
            // the same error reproduces the bug; a different one is an artifact of the reduction (e.g. a removed table)
            return expectedErrorMessage.equals(TestOracleUtils.getUnexpectedErrorMessage(unexpectedError));
        } catch (SQLException | RuntimeException e) {
            return false;
        }
        // the error no longer fires
        return false;
    }

    @Override
    public String getBugInformation() {
        return "-- On the database set up by the statements above, the following queries trigger an unexpected error"
                + " with message: " + expectedErrorMessage + System.lineSeparator() + queryLines;
    }
}
