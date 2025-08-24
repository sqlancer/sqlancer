package sqlancer;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import sqlancer.common.query.Query;

/**
 * A standalone tool to reduce bug-triggering SQL statements using the delta debugging algorithm.
 */
public class StandaloneReducer {
    private int partitionNum = 2;
    private final StateToReproduce originalState;
    private final DatabaseProvider<?, ?, ?> databaseProvider;

    public StandaloneReducer(Path serFilePath) throws Exception {
        this.originalState = StateToReproduce.deserialize(serFilePath);
        this.databaseProvider = originalState.getDatabaseProvider();
        if (this.databaseProvider == null) {
            throw new IllegalStateException("Failed to get database provider from .ser file");
        }
    }

    /**
     * Performs the main reduction algorithm using partition-based delta debugging.
     *
     * @return List of reduced SQL statements that still trigger bugs.
     */
    public List<Query<?>> reduce() throws Exception {
        List<Query<?>> queries = new ArrayList<>(originalState.getStatements());
        if (queries.size() <= 1) {
            return queries;
        }

        partitionNum = 2;
        while (queries.size() >= 2) {
            boolean changedInThisPass = false;
            List<Query<?>> result = tryReduction(queries);

            if (result.size() < queries.size()) {
                queries = result;
                changedInThisPass = true;
            }

            if (changedInThisPass) {
                partitionNum = 2;
            } else {
                if (partitionNum >= queries.size()) {
                    break;
                }
                partitionNum = Math.min(partitionNum * 2, queries.size());
            }
        }

        System.out.println("Reduction completed successfully!");
        System.out.println("Final size: " + queries.size() + " statements ("
                + String.format("%.1f", (1.0 - (double) queries.size() / originalState.getStatements().size()) * 100)
                + "% reduction)");
        System.out.println("Final queries:");
        for (Query<?> query : queries) {
            System.out.println(query.getQueryString());
        }

        return queries;
    }

    private List<Query<?>> tryReduction(List<Query<?>> queries) throws Exception {
        int start = 0;
        int subLength = queries.size() / partitionNum;

        while (start < queries.size()) {
            List<Query<?>> candidateQueries = new ArrayList<>(queries);
            int endPoint = Math.min(start + subLength, candidateQueries.size());
            candidateQueries.subList(start, endPoint).clear();

            if (testExceptionStillExists(candidateQueries)) {
                return candidateQueries;
            }

            start += subLength;
        }

        return queries;
    }

    // Test if bug still exists with reduced query set
    @SuppressWarnings("unchecked")
    private <G extends GlobalState<O, ?, C>, O extends DBMSSpecificOptions<?>, C extends SQLancerDBConnection> boolean testExceptionStillExists(
            List<Query<?>> queries) {
        try {
            DatabaseProvider<G, O, C> typedProvider = (DatabaseProvider<G, O, C>) databaseProvider;
            G globalState = typedProvider.getGlobalStateClass().getDeclaredConstructor().newInstance();

            try (C connection = typedProvider.createDatabase(globalState)) {
                globalState.setConnection(connection);
                for (Query<?> query : queries) {
                    try {
                        Query<C> typedQuery = (Query<C>) query;
                        typedQuery.execute(globalState);
                    } catch (Throwable e) {
                        // Any exception is considered a success
                        return true;
                    }
                }
                // No exception occurred
                return false;
            }
        } catch (Throwable e) {
            return true;
        }
    }

    public static void main(String[] args) {
        try {
            if (args.length == 0) {
                System.err.println(
                        "Usage: java -cp target/sqlancer-2.0.0.jar sqlancer.StandaloneReducer <path-to-ser-file>");
                System.exit(1);
            }
            StandaloneReducer reducer = new StandaloneReducer(Paths.get(args[0]));
            reducer.reduce();
        } catch (Throwable e) {
            System.err.println("ERROR: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}
