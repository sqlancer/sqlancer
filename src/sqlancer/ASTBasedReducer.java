package sqlancer;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import sqlancer.common.query.Query;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.transformations.RemoveClausesOfSelect;
import sqlancer.transformations.RemoveColumnsOfSelect;
import sqlancer.transformations.RemoveElementsOfExpressionList;
import sqlancer.transformations.RemoveRowsOfInsert;
import sqlancer.transformations.RemoveUnions;
import sqlancer.transformations.RoundDoubleConstant;
import sqlancer.transformations.SimplifyConstant;
import sqlancer.transformations.SimplifyExpressions;
import sqlancer.transformations.Transformation;

public class ASTBasedReducer<G extends GlobalState<O, ?, C>, O extends DBMSSpecificOptions<?>, C extends SQLancerDBConnection>
        implements Reducer<G> {

    private final DatabaseProvider<G, O, C> provider;

    @SuppressWarnings("unused")
    private G state;
    private G newGlobalState;
    private Reproducer<G> reproducer;

    private List<Query<C>> reducedStatements;
    // statement after reduction.

    public ASTBasedReducer(DatabaseProvider<G, O, C> provider) {
        this.provider = provider;
    }

    @SuppressWarnings("unchecked")
    private void updateStatements(String queryString, int index) {
        boolean couldAffectSchema = queryString.contains("CREATE TABLE") || queryString.contains("EXPLAIN");
        reducedStatements.set(index, (Query<C>) new SQLQueryAdapter(queryString, couldAffectSchema));
    }

    @SuppressWarnings("unchecked")
    @Override
    public void reduce(G state, Reproducer<G> reproducer, G newGlobalState) throws Exception {
        this.state = state;
        this.newGlobalState = newGlobalState;
        this.reproducer = reproducer;

        long maxReduceTime = state.getOptions().getMaxStatementReduceTime();
        long maxReduceSteps = state.getOptions().getMaxStatementReduceSteps();

        List<Query<?>> initialBugInducingStatements = state.getState().getStatements();
        newGlobalState.getState().setStatements(new ArrayList<>(initialBugInducingStatements));

        List<Transformation> transformations = new ArrayList<>();

        transformations.add(new RemoveUnions());
        transformations.add(new RemoveClausesOfSelect());
        transformations.add(new RemoveRowsOfInsert());
        transformations.add(new RemoveColumnsOfSelect());
        transformations.add(new RemoveElementsOfExpressionList());
        transformations.add(new SimplifyExpressions());
        transformations.add(new SimplifyConstant());
        transformations.add(new RoundDoubleConstant());

        Transformation.setBugJudgement(() -> {
            try {
                return this.bugStillTriggers();
            } catch (Exception ignored) {
            }
            return false;
        });

        boolean observeChange;
        reducedStatements = new ArrayList<>();
        for (Query<?> query : initialBugInducingStatements) {
            reducedStatements.add((Query<C>) query);
        }

        Instant startTime = Instant.now();
        reduceProcess: do {
            observeChange = false;
            for (Transformation t : transformations) {
                for (int i = 0; i < reducedStatements.size(); i++) {

                    Instant currentTime = Instant.now();
                    if (maxReduceTime != MainOptions.NO_REDUCE_LIMIT
                            && Duration.between(startTime, currentTime).getSeconds() >= maxReduceTime) {
                        break reduceProcess;
                    }

                    if (maxReduceSteps != MainOptions.NO_REDUCE_LIMIT
                            && Transformation.getReduceSteps() >= maxReduceSteps) {
                        break reduceProcess;
                    }

                    Query<?> query = reducedStatements.get(i);
                    boolean initFlag = t.init(query.getQueryString());
                    int index = i;
                    t.setStatementChangedCallBack((statementString) -> {
                        updateStatements(statementString, index);
                    });

                    if (!initFlag) {
                        newGlobalState.getLogger()
                                .logReducer("warning: failed parsing the statement at transformer : " + t);
                        continue;
                    }
                    t.apply();
                    observeChange |= t.changed();
                }
            }
        } while (observeChange);

        newGlobalState.getState().setStatements(new ArrayList<>(reducedStatements));
        newGlobalState.getLogger().logReduced(newGlobalState.getState());
    }

    public boolean bugStillTriggers() throws Exception {
        try (C con2 = provider.createDatabase(newGlobalState)) {
            newGlobalState.setConnection(con2);
            List<Query<C>> candidateStatements = new ArrayList<>(reducedStatements);
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
                    newGlobalState.getLogger().logReduced(newGlobalState.getState());
                    return true;
                }
            } catch (Throwable ignoredException) {

            }
        }
        return false;
    }
}
