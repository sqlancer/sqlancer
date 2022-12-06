package sqlancer;

import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.StateToReproduce.OracleRunReproductionState;
import sqlancer.common.oracle.CompositeTestOracle;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.schema.AbstractSchema;

public abstract class ProviderAdapter<G extends GlobalState<O, ? extends AbstractSchema<G, ?>, C>, O extends DBMSSpecificOptions<? extends OracleFactory<G>>, C extends SQLancerDBConnection>
        implements DatabaseProvider<G, O, C> {

    private final Class<G> globalClass;
    private final Class<O> optionClass;

    private Long executedQueryCount;

    public ProviderAdapter(Class<G> globalClass, Class<O> optionClass) {
        this.globalClass = globalClass;
        this.optionClass = optionClass;
        this.executedQueryCount = 0L;
    }

    @Override
    public StateToReproduce getStateToReproduce(String databaseName) {
        return new StateToReproduce(databaseName, this);
    }

    @Override
    public Class<G> getGlobalStateClass() {
        return globalClass;
    }

    @Override
    public Class<O> getOptionClass() {
        return optionClass;
    }

    @Override
    public boolean addQueryPlan(G globalState, String select) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getQueryPlan(G globalState, String selectStr) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean mutateTables(G globalState) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean addRowsToAllTables(G globalState) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public Reproducer<G> generateAndTestDatabase(G globalState) throws Exception {
        try {
            generateDatabase(globalState);
            checkViewsAreValid(globalState);
            globalState.getManager().incrementCreateDatabase();

            TestOracle<G> oracle = getTestOracle(globalState);
            for (int i = 0; i < globalState.getOptions().getNrQueries(); i++) {
                try (OracleRunReproductionState localState = globalState.getState().createLocalState()) {
                    assert localState != null;
                    try {
                        oracle.check();
                        globalState.getManager().incrementSelectQueryCount();
                    } catch (IgnoreMeException e) {

                    } catch (AssertionError e) {
                        Reproducer<G> reproducer = oracle.getLastReproducer();
                        if (reproducer != null) {
                            return reproducer;
                        }
                        throw e;
                    }
                    assert localState != null;
                    localState.executedWithoutError();
                }
            }
        } finally {
            globalState.getConnection().close();
        }
        return null;
    }

    @Override
    public void qpg(G globalState) throws Exception {
        try {
            generateDatabase(globalState);
            checkViewsAreValid(globalState);
            globalState.getManager().incrementCreateDatabase();

            while (executedQueryCount < globalState.getOptions().getNrQueries()) {
                int numOfNoNewQueryPlans = 0;
                // Check the oracles
                TestOracle<G> oracle = getTestOracle(globalState);
                while (true) {
                    try (OracleRunReproductionState localState = globalState.getState().createLocalState()) {
                        assert localState != null;
                        try {
                            String query = oracle.check();
                            executedQueryCount += 1;
                            if (addQueryPlan(globalState, query)) {
                                numOfNoNewQueryPlans = 0;
                            } else {
                                numOfNoNewQueryPlans++;
                            }
                            globalState.getManager().incrementSelectQueryCount();
                        } catch (IgnoreMeException e) {

                        }
                        assert localState != null;
                        localState.executedWithoutError();
                    }
                    if (numOfNoNewQueryPlans > globalState.getOptions().getQPGMinInterval()) {
                        break;
                    }
                }
                mutateTables(globalState);
            }
        } finally {
            globalState.getConnection().close();
        }
    }

    protected abstract void checkViewsAreValid(G globalState) throws SQLException;

    protected TestOracle<G> getTestOracle(G globalState) throws Exception {
        List<? extends OracleFactory<G>> testOracleFactory = globalState.getDbmsSpecificOptions()
                .getTestOracleFactory();
        boolean testOracleRequiresMoreThanZeroRows = testOracleFactory.stream()
                .anyMatch(p -> p.requiresAllTablesToContainRows());
        boolean userRequiresMoreThanZeroRows = globalState.getOptions().testOnlyWithMoreThanZeroRows();
        boolean checkZeroRows = testOracleRequiresMoreThanZeroRows || userRequiresMoreThanZeroRows;
        if (checkZeroRows && globalState.getSchema().containsTableWithZeroRows(globalState)) {
            if (globalState.getOptions().enableQPG()) {
                addRowsToAllTables(globalState);
            } else {
                throw new IgnoreMeException();
            }
        }
        if (testOracleFactory.size() == 1) {
            return testOracleFactory.get(0).create(globalState);
        } else {
            return new CompositeTestOracle<G>(testOracleFactory.stream().map(o -> {
                try {
                    return o.create(globalState);
                } catch (Exception e1) {
                    throw new AssertionError(e1);
                }
            }).collect(Collectors.toList()), globalState);
        }
    }

    public abstract void generateDatabase(G globalState) throws Exception;

}
