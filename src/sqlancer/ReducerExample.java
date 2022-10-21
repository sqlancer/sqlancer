package sqlancer;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import sqlancer.FoundBugException.Reproducer;
import sqlancer.common.query.Query;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.sqlite3.SQLite3GlobalState;

public class ReducerExample<G extends GlobalState<O, ?, C>,
        O extends DBMSSpecificOptions<?>, C extends SQLancerDBConnection>
        implements Reducer <G, O, C> {
    private final DatabaseProvider<G, O, C> provider;
    private boolean observedChange;

    private static final String[] TOKENS = new String[] {
        "OR IGNORE", "OR ABORT", "OR ROLLBACK", "OR FAIL", "TEMP", "TEMPORARY",
        "UNIQUE", "NOT NULL", "COLLATE BINARY", "COLLATE NOCASE",
        "COLLATE RTRIM", "INT", "REAL", "TEXT", "IF NOT EXISTS", "UNINDEXED"};

    public ReducerExample(DatabaseProvider<G, O, C> provider) {
        this.provider = provider;
    }

    @SuppressWarnings("unchecked")
    public void reduce(G state, Reproducer reproducer, G newGlobalState) throws Exception {
        
        List<Query<C>> knownToReproduceBugStatements = new ArrayList<Query<C>>();
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

        for (String s : TOKENS) {
            knownToReproduceBugStatements = tryReplaceToken(state, reproducer, newGlobalState,
                    knownToReproduceBugStatements, " " + s, "");
        }

        System.out.println("Reduced query:");
        printQueries(knownToReproduceBugStatements);
    }

    @SuppressWarnings("unchecked")
    private List<Query<C>> tryReplaceToken(G state, Reproducer reproducer, G newGlobalState,
            List<Query<C>> knownToReproduceBugStatements, String target, String replaceBy) throws Exception {
        do {
            observedChange = false;
            knownToReproduceBugStatements = tryReduction(state, reproducer, newGlobalState,
                    knownToReproduceBugStatements, (candidateStatements, i) -> {
                        Query<C> statement = candidateStatements.get(i);
                        if (statement.getQueryString().contains(target)) {
                            candidateStatements.set(i, (Query<C>) new SQLQueryAdapter(
                                    statement.getQueryString().replace(target, replaceBy), true));
                            return true;
                        }
                        return false;
                    }

            );
        } while (observedChange);
        return knownToReproduceBugStatements;
    }

    private List<Query<C>> tryReduction(G state, Reproducer reproducer, G newGlobalState,
            List<Query<C>> knownToReproduceBugStatements,
            BiFunction<List<Query<C>>, Integer, Boolean> reductionOperation) throws Exception {

        for (int i = 0; i < knownToReproduceBugStatements.size(); i++) {
            try (C con2 = provider.createDatabase(newGlobalState)) {
                newGlobalState.setConnection(con2);
                List<Query<C>> candidateStatements = new ArrayList<>(knownToReproduceBugStatements);
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
                    if (reproducer.bugStillTriggers((SQLite3GlobalState) newGlobalState)) {
                        observedChange = true;
                        knownToReproduceBugStatements = candidateStatements;
                        reproducer.outputHook((SQLite3GlobalState) newGlobalState);
                        state.getLogger().logReduced(newGlobalState.getState());
                    }
                } catch (Throwable ignoredException) {

                }
            }
        }
        return knownToReproduceBugStatements;
    }

    private void printQueries(List<Query<C>> statements) {
        System.out.println("===============================");
        for (Query<?> q : statements) {
            System.out.println(q.getLogString());
        }
        System.out.println("===============================");
    }

}
