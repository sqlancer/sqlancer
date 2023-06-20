package sqlancer;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import sqlancer.common.query.Query;

public class StatementReducer<G extends GlobalState<O, ?, C>, O extends DBMSSpecificOptions<?>, C extends SQLancerDBConnection>
        implements Reducer<G> {
    private final DatabaseProvider<G, O, C> provider;
    private boolean observedChange;
    private int partitionNum;

    public StatementReducer(DatabaseProvider<G, O, C> provider) {
        this.provider = provider;
    }

    private boolean hasNotReachedLimit(long curr, long limit) {
        if (limit == MainOptions.NO_REDUCE_LIMIT) {
            return true;
        }
        return curr < limit;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void reduce(G state, Reproducer<G> reproducer, G newGlobalState) throws Exception {

        long maxReduceTime = state.getOptions().getMaxStatementReduceTime();
        long maxReduceSteps = state.getOptions().getMaxStatementReduceSteps();

        List<Query<C>> knownToReproduceBugStatements = new ArrayList<>();
        for (Query<?> stat : state.getState().getStatements()) {
            knownToReproduceBugStatements.add((Query<C>) stat);
        }

        System.out.println("Starting query:");
        printQueries(knownToReproduceBugStatements);
        System.out.println();

        if (knownToReproduceBugStatements.size() <= 1) {
            return;
        }

        Instant timeOfReductionBegins = Instant.now();
        long currentReduceSteps = 0;
        long currentReduceTime = 0;
        partitionNum = 2;

        while (knownToReproduceBugStatements.size() >= 2 && hasNotReachedLimit(currentReduceSteps, maxReduceSteps)
                && hasNotReachedLimit(currentReduceTime, maxReduceTime)) {
            observedChange = false;

            knownToReproduceBugStatements = tryReduction(state, reproducer, newGlobalState,
                    knownToReproduceBugStatements);
            if (!observedChange) {
                if (partitionNum == knownToReproduceBugStatements.size()) {
                    break;
                }
                // increase the search granularity
                partitionNum = Math.min(partitionNum * 2, knownToReproduceBugStatements.size());

                currentReduceSteps++;
                Instant currentInstant = Instant.now();
                currentReduceTime = Duration.between(currentInstant, timeOfReductionBegins).getSeconds();
            }

        }

        System.out.println("Reduced query:");
        printQueries(knownToReproduceBugStatements);
        newGlobalState.getState().setStatements(new ArrayList<>(knownToReproduceBugStatements));
    }

    private List<Query<C>> tryReduction(G state, // NOPMD
            Reproducer<G> reproducer, G newGlobalState, List<Query<C>> knownToReproduceBugStatements) throws Exception {

        List<Query<C>> statements = knownToReproduceBugStatements;

        int start = 0;
        int subLength = statements.size() / partitionNum;
        while (start < statements.size()) {
            // newStatements = candidate[:start] + candidate[start+subLength:]
            // in other word, remove [start, start+subLength) from candidates
            try (C con2 = provider.createDatabase(newGlobalState)) {
                newGlobalState.setConnection(con2);
                List<Query<C>> candidateStatements = new ArrayList<>(statements);
                candidateStatements.subList(start, start + subLength).clear();
                newGlobalState.getState().setStatements(new ArrayList<>(candidateStatements));

                for (Query<C> s : candidateStatements) {
                    try {
                        s.execute(newGlobalState);
                    } catch (Throwable ignoredException) {
                        // ignore
                    }
                }
                try {
                    if (reproducer.bugStillTriggers(newGlobalState)) {
                        observedChange = true;
                        statements = candidateStatements;
                        partitionNum = Math.max(partitionNum - 1, 2);
                        break;
                        // reproducer.outputHook((SQLite3GlobalState) newGlobalState);
                        // state.getLogger().logReduced(newGlobalState.getState());
                    }
                } catch (Throwable ignoredException) {

                }
            }
            start = start + subLength;
        }
        return statements;
    }

    private void printQueries(List<Query<C>> statements) {
        System.out.println("===============================");
        for (Query<?> q : statements) {
            System.out.println(q.getLogString());
        }
        System.out.println("===============================");
    }

}
