package sqlancer;

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

    public ProviderAdapter(Class<G> globalClass, Class<O> optionClass) {
        this.globalClass = globalClass;
        this.optionClass = optionClass;
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
    public void generateAndTestDatabase(G globalState) throws Exception {
        try {
            generateDatabase(globalState);
            checkViewsAreValid(globalState);
            globalState.getManager().incrementCreateDatabase();

            TestOracle oracle = getTestOracle(globalState);
            for (int i = 0; i < globalState.getOptions().getNrQueries(); i++) {
                try (OracleRunReproductionState localState = globalState.getState().createLocalState()) {
                    assert localState != null;
                    try {
                        oracle.check();
                        globalState.getManager().incrementSelectQueryCount();
                    } catch (IgnoreMeException e) {

                    }
                    assert localState != null;
                    localState.executedWithoutError();
                }
            }
        } finally {
            globalState.getConnection().close();
        }
    }

    protected abstract void checkViewsAreValid(G globalState);

    protected TestOracle getTestOracle(G globalState) throws Exception {
        List<? extends OracleFactory<G>> testOracleFactory = globalState.getDbmsSpecificOptions()
                .getTestOracleFactory();
        boolean testOracleRequiresMoreThanZeroRows = testOracleFactory.stream()
                .anyMatch(p -> p.requiresAllTablesToContainRows());
        boolean userRequiresMoreThanZeroRows = globalState.getOptions().testOnlyWithMoreThanZeroRows();
        boolean checkZeroRows = testOracleRequiresMoreThanZeroRows || userRequiresMoreThanZeroRows;
        if (checkZeroRows && globalState.getSchema().containsTableWithZeroRows(globalState)) {
            throw new IgnoreMeException();
        }
        if (testOracleFactory.size() == 1) {
            return testOracleFactory.get(0).create(globalState);
        } else {
            return new CompositeTestOracle(testOracleFactory.stream().map(o -> {
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
