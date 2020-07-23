package sqlancer;

import java.io.FileWriter;
import java.sql.SQLException;

import sqlancer.StateToReproduce.OracleRunReproductionState;

public abstract class ProviderAdapter<G extends GlobalState<O, ?>, O> implements DatabaseProvider<G, O> {

    private final Class<G> globalClass;
    private final Class<O> optionClass;

    public ProviderAdapter(Class<G> globalClass, Class<O> optionClass) {
        this.globalClass = globalClass;
        this.optionClass = optionClass;
    }

    @Override
    public void printDatabaseSpecificState(FileWriter writer, StateToReproduce state) {

    }

    @Override
    public StateToReproduce getStateToReproduce(String databaseName) {
        return new StateToReproduce(databaseName);
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

    protected abstract TestOracle getTestOracle(G globalState) throws SQLException;

    public abstract void generateDatabase(G globalState) throws SQLException;

}
