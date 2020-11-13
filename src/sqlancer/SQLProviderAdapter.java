package sqlancer;

import sqlancer.common.log.LoggableFactory;
import sqlancer.common.log.SQLLoggableFactory;

public abstract class SQLProviderAdapter<G extends GlobalState<O, ?>, O extends DBMSSpecificOptions<? extends OracleFactory<G>>>
        extends ProviderAdapter<G, O> {
    public SQLProviderAdapter(Class<G> globalClass, Class<O> optionClass) {
        super(globalClass, optionClass);
    }

    @Override
    public LoggableFactory getLoggableFactory() {
        return new SQLLoggableFactory();
    }
}
