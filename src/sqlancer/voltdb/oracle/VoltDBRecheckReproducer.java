package sqlancer.voltdb.oracle;

import java.sql.SQLException;

import sqlancer.Reproducer;
import sqlancer.voltdb.VoltDBProvider.VoltDBGlobalState;

/**
 * Re-runs an oracle's queries and reports the bug as still present while its check still finds a problem. It reports
 * the problem of the last run that still found one, so that after reduction it is that of the reduced database.
 */
final class VoltDBRecheckReproducer implements Reproducer<VoltDBGlobalState> {

    private final Check check;
    private final String queries;
    private String problem;

    // queries: the queries as commented lines, each ending in a line separator
    VoltDBRecheckReproducer(Check check, String queries, String problem) {
        this.check = check;
        this.queries = queries;
        this.problem = problem;
    }

    @Override
    public boolean bugStillTriggers(VoltDBGlobalState globalState) {
        try {
            String rerun = check.run(globalState);
            if (rerun != null) {
                problem = rerun;
                return true;
            }
            return false;
        } catch (Exception | AssertionError e) {
            // an error, expected or not, is an artifact of the reduction, such as a removed table
            return false;
        }
    }

    @Override
    public String getBugInformation() {
        return "-- On the database set up by the statements above, these queries disagree:" + System.lineSeparator()
                + queries + "-- " + problem.replace(System.lineSeparator(), System.lineSeparator() + "-- ")
                + System.lineSeparator();
    }

    // Runs the queries and returns the problem they show, or null
    interface Check {
        String run(VoltDBGlobalState state) throws SQLException;
    }
}
