package sqlancer;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import sqlancer.common.query.Query;

public class StatementReducer<G extends GlobalState<O, ?, C>, O extends DBMSSpecificOptions<?>, C extends SQLancerDBConnection>
        implements Reducer<G> {
    private final DatabaseProvider<G, O, C> provider;
    private boolean observedChange;

    public StatementReducer(DatabaseProvider<G, O, C> provider) {
        this.provider = provider;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void reduce(G state, Reproducer<G> reproducer, G newGlobalState) throws Exception {

        List<Query<C>> knownToReproduceBugStatements = new ArrayList<>();
        for (Query<?> stat : state.getState().getStatements()) {
            knownToReproduceBugStatements.add((Query<C>) stat);
        }
        System.out.println("Starting query:");
        printQueries(knownToReproduceBugStatements);
        System.out.println();

        do {
            observedChange = false;
            knownToReproduceBugStatements = tryReduction(state, reproducer, newGlobalState,
                    knownToReproduceBugStatements, (candidateStatements, i) -> {
                        candidateStatements.remove((int) i);
                        return true;
                    });
        } while (observedChange);

        System.out.println("Reduced query:");
        printQueries(knownToReproduceBugStatements);
    }

    private List<Query<C>> tryReduction(G state, // NOPMD
            Reproducer<G> reproducer, G newGlobalState, List<Query<C>> knownToReproduceBugStatements,
            BiFunction<List<Query<C>>, Integer, Boolean> reductionOperation) throws Exception {

        List<Query<C>> statements = knownToReproduceBugStatements;
        for (int i = 0; i < statements.size(); i++) {
            try (C con2 = provider.createDatabase(newGlobalState)) {
                newGlobalState.setConnection(con2);
                List<Query<C>> candidateStatements = new ArrayList<>(statements);
                if (!reductionOperation.apply(candidateStatements, i)) {
                    continue;
                }
                newGlobalState.getState().setStatements(candidateStatements.stream().collect(Collectors.toList()));
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
                        // reproducer.outputHook((SQLite3GlobalState) newGlobalState);
                        // state.getLogger().logReduced(newGlobalState.getState());
                    }
                } catch (Throwable ignoredException) {

                }
            }
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
