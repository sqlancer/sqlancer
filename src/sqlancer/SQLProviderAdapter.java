package sqlancer;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;

import sqlancer.common.log.LoggableFactory;
import sqlancer.common.log.SQLLoggableFactory;
import sqlancer.common.query.Query;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.schema.AbstractSchema;
import sqlancer.common.schema.AbstractTable;

public abstract class SQLProviderAdapter<G extends SQLGlobalState<O, ? extends AbstractSchema<G, ?>>, O extends DBMSSpecificOptions<? extends OracleFactory<G>>>
        extends ProviderAdapter<G, O, SQLConnection> {

    private boolean observedChange = false;

    public SQLProviderAdapter(Class<G> globalClass, Class<O> optionClass) {
        super(globalClass, optionClass);
    }

    @Override
    public LoggableFactory getLoggableFactory() {
        return new SQLLoggableFactory();
    }

    @Override
    protected void checkViewsAreValid(G globalState) {
        List<? extends AbstractTable<?, ?, ?>> views = globalState.getSchema().getViews();
        for (AbstractTable<?, ?, ?> view : views) {
            SQLQueryAdapter q = new SQLQueryAdapter("SELECT 1 FROM " + view.getName() + " LIMIT 1");
            try {
                q.execute(globalState);
            } catch (Throwable t) {
                throw new IgnoreMeException();
            }
        }
    }

    @Override
    public void reduceDatabase(FoundBugException e, G stateToReduce, G newGlobalState) throws Exception {

        FoundBugException.Reproducer<G, O> reproducer = e.getReproducer();

        List<SQLQueryAdapter> knownToReproduceBugStatements = new ArrayList<>();
        for (Query<?> stat : stateToReduce.getState().getStatements()) {
            knownToReproduceBugStatements.add((SQLQueryAdapter) stat);
        }
        // Iterate until fixpoint.
        do {
            observedChange = false;
            System.out.println(observedChange);
            knownToReproduceBugStatements = tryReduction(reproducer, newGlobalState,
                    knownToReproduceBugStatements, (candidateStatements, i) -> {
                        candidateStatements.remove((int) i);
                        return true;
                    });
        } while (observedChange);

        for (String s : new String[] { "OR IGNORE", "OR ABORT", "OR ROLLBACK", "OR FAIL", "TEMP",
                "TEMPORARY", "UNIQUE", "NOT NULL", "COLLATE BINARY", "COLLATE NOCASE", "COLLATE RTRIM",
                "INT", "REAL", "TEXT", "IF NOT EXISTS", "UNINDEXED" }) {
            knownToReproduceBugStatements = tryReplaceToken(reproducer, newGlobalState,
                    knownToReproduceBugStatements, " " + s, "");
        }
    }

    private List<SQLQueryAdapter> tryReplaceToken(FoundBugException.Reproducer<G, O> reproducer, G newGlobalState,
                                           List<SQLQueryAdapter> knownToReproduceBugStatements, String target, String replaceBy) throws Exception {
        do {
            observedChange = false;
            knownToReproduceBugStatements = tryReduction(reproducer, newGlobalState,
                    knownToReproduceBugStatements, (candidateStatements, i) -> {
                        SQLQueryAdapter statement = candidateStatements.get(i);
                        if (statement.getQueryString().contains(target)) {
                            candidateStatements.set(i, new SQLQueryAdapter(
                                    statement.getQueryString().replace(target, replaceBy), true));
                            return true;
                        }
                        return false;
                    }
            );
        } while (observedChange);
        return knownToReproduceBugStatements;
    }

    private List<SQLQueryAdapter> tryReduction(FoundBugException.Reproducer<G, O> reproducer, G newGlobalState,
                                        List<SQLQueryAdapter> knownToReproduceBugStatements,
                                        BiFunction<List<SQLQueryAdapter>, Integer, Boolean> reductionOperation) throws Exception {
        for (int i = 0; i < knownToReproduceBugStatements.size(); i++) {
            try (SQLConnection con2 = this.createDatabase(newGlobalState)) {
                newGlobalState.setConnection(con2);
                List<SQLQueryAdapter> candidateStatements = new ArrayList<>(knownToReproduceBugStatements);
                if (!reductionOperation.apply(candidateStatements, i)) {
                    continue;
                }
                newGlobalState.getState().setStatements(new ArrayList<>(candidateStatements));
                for (SQLQueryAdapter s : candidateStatements) {
                    try {
                        s.execute(newGlobalState);
                    } catch (Throwable ignoredException) {
                        // ignore
                    }
                }
                try {
                    if (reproducer.bugStillTriggers(newGlobalState)) {
                        observedChange = true;
                        knownToReproduceBugStatements = candidateStatements;
                        reproducer.outputHook(newGlobalState);
                    }
                } catch (Throwable ignoredException) {

                }
            }
        }
        return knownToReproduceBugStatements;
    }
}
