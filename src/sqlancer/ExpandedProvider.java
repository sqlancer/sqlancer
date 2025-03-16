package sqlancer;

import sqlancer.common.schema.AbstractSchema;

public abstract class ExpandedProvider<G extends SQLGlobalState<O, ? extends AbstractSchema<G, ?>>, O extends DBMSSpecificOptions<? extends OracleFactory<G>>>
        extends SQLProviderAdapter<G, O> {

    public static boolean generateOnlyKnown;

    protected String entryURL;
    protected String username;
    protected String password;
    protected String entryPath;
    protected String host;
    protected int port;
    protected String testURL;
    protected String databaseName;
    protected String createDatabaseCommand;
    protected String extensionsList;

    protected ExpandedProvider(Class<G> globalClass, Class<O> optionClass) {
        super(globalClass, optionClass);
    }
}
