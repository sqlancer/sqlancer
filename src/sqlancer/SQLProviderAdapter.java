package sqlancer;

import java.util.List;

import sqlancer.common.log.LoggableFactory;
import sqlancer.common.log.SQLLoggableFactory;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.schema.AbstractSchema;
import sqlancer.common.schema.AbstractTable;

public abstract class SQLProviderAdapter<G extends SQLGlobalState<O, ? extends AbstractSchema<G, ?>>, O extends DBMSSpecificOptions<? extends OracleFactory<G>>>
        extends ProviderAdapter<G, O, SQLConnection> {
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
}
