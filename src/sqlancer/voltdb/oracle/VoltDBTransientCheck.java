package sqlancer.voltdb.oracle;

import sqlancer.IgnoreMeException;
import sqlancer.Reproducer;
import sqlancer.common.oracle.TestOracle;
import sqlancer.voltdb.VoltDBProvider.VoltDBGlobalState;

/**
 * ENG-30602: now and then, under a stream of catalog updates from other connections, a query receives another
 * statement's result (a one-column table, or other rows), and the same query run again a moment later on the same data
 * is right. While --avoid-known-bugs is true, this wraps the database's oracles: when a check fails and the failing
 * oracle has a reproducer, the reproducer runs the queries again at once. If they no longer fail, the failure is
 * printed as "ENG-30602 transient" (which run-sqlancer.sh counts in its summary) and the check is skipped; if they
 * still fail, the failure stands. Oracles without a reproducer (SCHEMA, GEO) are not retried.
 */
public class VoltDBTransientCheck implements TestOracle<VoltDBGlobalState> {

    private final TestOracle<VoltDBGlobalState> oracle;
    private final VoltDBGlobalState state;

    public VoltDBTransientCheck(TestOracle<VoltDBGlobalState> oracle, VoltDBGlobalState state) {
        this.oracle = oracle;
        this.state = state;
    }

    @Override
    public void check() throws Exception {
        try {
            oracle.check();
        } catch (AssertionError failure) {
            Reproducer<VoltDBGlobalState> reproducer = oracle.getLastReproducer();
            if (reproducer != null && !stillFails(reproducer)) {
                String first = String.valueOf(failure.getMessage()).split("\\R", 2)[0];
                System.out.println("ENG-30602 transient: " + state.getDatabaseName() + ": " + first + " -- query: "
                        + oracle.getLastQueryString());
                throw new IgnoreMeException();
            }
            throw failure;
        }
    }

    // whether the failure recurs; an error while rerunning, such as a node failure, counts as recurring
    private boolean stillFails(Reproducer<VoltDBGlobalState> reproducer) {
        try {
            return reproducer.bugStillTriggers(state);
        } catch (Exception | AssertionError e) {
            return true;
        }
    }

    @Override
    public Reproducer<VoltDBGlobalState> getLastReproducer() {
        return oracle.getLastReproducer();
    }

    @Override
    public String getLastQueryString() {
        return oracle.getLastQueryString();
    }

}
