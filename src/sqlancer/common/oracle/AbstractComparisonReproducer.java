package sqlancer.common.oracle;

import java.sql.SQLException;

import sqlancer.Reproducer;
import sqlancer.SQLGlobalState;

/**
 * Shared skeleton for the {@link Reproducer}s of oracles that detect a bug by comparing two evaluations of a
 * semantically-equivalent pair (e.g. {@link EETOracle}, {@link NoRECOracle}, {@link TLPWhereOracle}). Reduction re-runs
 * both sides against the reduced database and reports whether they still disagree.
 *
 * <p>
 * The separate case where the original bug was an unexpected DBMS error rather than a mismatch is handled by
 * {@link UnexpectedErrorReproducer}, so a subclass here deals only with comparing two sides and never with error
 * handling.
 *
 * <p>
 * This class owns the compare-and-report control flow and the {@link #getBugInformation()} header. Subclasses supply
 * how each side is evaluated, how the two are compared, and how the failing queries are rendered in the reduced test
 * case.
 *
 * @param <G>
 *            the DBMS-specific global state class
 * @param <R>
 *            the type each side evaluates to (e.g. a result set as a list of strings, a row count)
 */
public abstract class AbstractComparisonReproducer<G extends SQLGlobalState<?, ?>, R> implements Reproducer<G> {

    /**
     * Evaluates the original side against the reduced database.
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
     * Evaluates the transformed side against the reduced database.
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
            transformed = evaluateTransformed(globalState);
        } catch (AssertionError | SQLException | RuntimeException e) {
            // any failure re-running the two sides means this reduced database no longer shows the mismatch
            return false;
        }
        return sidesDiffer(original, transformed, globalState);
    }

    @Override
    public final String getBugInformation() {
        StringBuilder sb = new StringBuilder();
        sb.append(mismatchHeaderLine()).append(System.lineSeparator());
        appendQueryLines(sb);
        return sb.toString();
    }

    /**
     * The header line (without trailing line separator) describing the mismatch. For example, "-- On the database set
     * up by the statements above, the result sets of the following queries mismatch:".
     *
     * @return the mismatch header line
     */
    protected abstract String mismatchHeaderLine();

    /**
     * Appends the failing queries to {@code sb}, one commented line each, so the reduced test case is self-contained.
     *
     * @param sb
     *            the builder to append to
     */
    protected abstract void appendQueryLines(StringBuilder sb);
}
