package sqlancer;

import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.StateToReproduce.OracleRunReproductionState;
import sqlancer.common.oracle.CompositeTestOracle;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.QueryAdapter;
import sqlancer.common.schema.AbstractTable;

public abstract class ProviderAdapter<G extends GlobalState<O, ?>, O extends DBMSSpecificOptions<? extends OracleFactory<G>>>
        implements DatabaseProvider<G, O> {

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
    public void generateAndTestDatabase(G globalState) throws SQLException {
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

    private void checkViewsAreValid(G globalState) {
        List<? extends AbstractTable<?, ?>> views = globalState.getSchema().getViews();
        for (AbstractTable<?, ?> view : views) {
            QueryAdapter q = new QueryAdapter("SELECT 1 FROM " + view.getName() + " LIMIT 1");
            try {
                q.execute(globalState);
            } catch (Throwable t) {
                throw new IgnoreMeException();
            }
        }
    }

    protected TestOracle getTestOracle(G globalState) throws SQLException {
        List<? extends OracleFactory<G>> testOracleFactory = globalState.getDmbsSpecificOptions()
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
                } catch (SQLException e1) {
                    throw new AssertionError(e1);
                }
            }).collect(Collectors.toList()), globalState);
        }
    }

    public abstract void generateDatabase(G globalState) throws SQLException;

}
