package sqlancer.common.oracle;

import java.sql.SQLException;

import sqlancer.Reproducer;
import sqlancer.SQLGlobalState;

/**
 * Shared skeleton for the {@link Reproducer}s of oracles that detect a bug by comparing two evaluations of a
 * semantically-equivalent pair (e.g. {@link EETOracle}, {@link NoRECOracle}, {@link TLPWhereOracle}. All of these
 * reduce the bug the same way: re-evaluate both sides against the reduced database and report whether they still
 * disagree (or, when the original bug was an unexpected DBMS error, whether that same error still fires).
 *
 * <p>
 * This class owns that control flow (including distinguishing a still-reproducing error from an unrelated one
 * introduced by the reduction) and the {@link #getBugInformation()} header. Subclasses supply the parts specific to
 * their oracle: how each side is evaluated, how the two are compared, and how the failing queries are rendered in the
 * reduced test case.
 *
 * @param <G>
 *            the DBMS-specific global state class
 * @param <R>
 *            the type each side evaluates to (e.g. a result set as a list of strings, a row count, a post-image)
 */
public abstract class AbstractComparisonReproducer<G extends SQLGlobalState<?, ?>, R> implements Reproducer<G> {

    /**
     * The message of the unexpected DBMS error the original bug was, or {@code null} if the original bug was a
     * comparison mismatch rather than an error.
     */
    protected final String expectedErrorMessage;

    protected AbstractComparisonReproducer(String expectedErrorMessage) {
        this.expectedErrorMessage = expectedErrorMessage;
    }

    /**
     * Whether the recorded bug has a transformed (second) side. It does not when the bug was a DBMS error triggered by
     * the original query alone, in which case there is no second side to evaluate or compare.
     *
     * @return {@code true} if {@link #evaluateTransformed} should be called
     */
    protected abstract boolean hasTransformedSide();

    /**
     * Evaluates the original side against the (reduced) database.
     *
     * @param globalState
     *            the state whose connection points at the reduced database
     *
     * @return the original side's value
     *
     * @throws SQLException
     *             if a DBMS interaction fails
     */
    protected abstract R evaluateOriginal(G globalState) throws SQLException;

    /**
     * Evaluates the transformed side against the (reduced) database. Only called when {@link #hasTransformedSide()} is
     * {@code true}.
     *
     * @param globalState
     *            the state whose connection points at the reduced database
     *
     * @return the transformed side's value
     *
     * @throws SQLException
     *             if a DBMS interaction fails
     */
    protected abstract R evaluateTransformed(G globalState) throws SQLException;

    /**
     * Whether the two evaluated sides disagree in the way that constitutes the bug.
     *
     * @param original
     *            the original side's value
     * @param transformed
     *            the transformed side's value
     * @param globalState
     *            the state the sides were evaluated against
     *
     * @return {@code true} if the sides differ (i.e. the bug still triggers)
     */
    protected abstract boolean sidesDiffer(R original, R transformed, G globalState);

    @Override
    public final boolean bugStillTriggers(G globalState) {
        R original;
        R transformed;
        try {
            original = evaluateOriginal(globalState);
            if (!hasTransformedSide()) {
                // the original bug was a DBMS error on the original query alone, which no longer occurs
                return false;
            }
            transformed = evaluateTransformed(globalState);
        } catch (AssertionError unexpectedError) {
            // a DBMS error reproduces the bug only if the original failure was the same error;
            // other errors are artifacts of the reduction (e.g., a removed CREATE TABLE)
            return expectedErrorMessage != null
                    && expectedErrorMessage.equals(TestOracleUtils.getUnexpectedErrorMessage(unexpectedError));
        } catch (SQLException | RuntimeException e) {
            return false;
        }
        if (expectedErrorMessage != null) {
            // the original bug was a DBMS error, which no longer occurs
            return false;
        }
        return sidesDiffer(original, transformed, globalState);
    }

    @Override
    public final String getBugInformation() {
        StringBuilder sb = new StringBuilder();
        if (expectedErrorMessage != null) {
            sb.append("-- On the database set up by the statements above, the following queries trigger an"
                    + " unexpected error with message: ").append(expectedErrorMessage).append(System.lineSeparator());
        } else {
            sb.append(mismatchHeaderLine()).append(System.lineSeparator());
        }
        appendQueryLines(sb);
        return sb.toString();
    }

    /**
     * The header line (without trailing line separator) describing the mismatch, used when the original bug was a
     * comparison mismatch rather than an error. For example, "-- On the database set up by the statements above, the
     * result sets of the following queries mismatch:".
     *
     * @return the mismatch header line
     */
    protected abstract String mismatchHeaderLine();

    /**
     * Appends the failing queries (or statements) to {@code sb}, one commented line each, so the reduced test case is
     * self-contained. Called for both the mismatch and the error case, after the header.
     *
     * @param sb
     *            the builder to append to
     */
    protected abstract void appendQueryLines(StringBuilder sb);
}
