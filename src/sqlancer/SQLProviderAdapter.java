package sqlancer;

import sqlancer.common.log.LoggableFactory;
import sqlancer.common.log.SQLLoggableFactory;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.schema.AbstractRelationalTable;

import java.util.List;

public abstract class SQLProviderAdapter<G extends SQLGlobalState<O, ?>, O extends DBMSSpecificOptions<? extends OracleFactory<G>>>
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
        List<? extends AbstractRelationalTable<?, ?, G>> views = globalState.getSchema().getViews();
        for (AbstractRelationalTable<?, ?, G> view : views) {
            SQLQueryAdapter q = new SQLQueryAdapter("SELECT 1 FROM " + view.getName() + " LIMIT 1");
            try {
                q.execute(globalState);
            } catch (Throwable t) {
                throw new IgnoreMeException();
            }
        }
    }
}
