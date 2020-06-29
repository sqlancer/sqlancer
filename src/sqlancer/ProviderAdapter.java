package sqlancer;

import java.io.FileWriter;

public abstract class ProviderAdapter<G extends GlobalState<O>, O> implements DatabaseProvider<G, O> {

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

}
