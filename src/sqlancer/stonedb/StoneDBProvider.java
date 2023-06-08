package sqlancer.stonedb;

import com.google.auto.service.AutoService;
import sqlancer.DatabaseProvider;
import sqlancer.SQLConnection;
import sqlancer.SQLGlobalState;
import sqlancer.SQLProviderAdapter;

@AutoService(DatabaseProvider.class)
public class StoneDBProvider extends SQLProviderAdapter<StoneDBProvider.StoneDBGlobalState, StoneDBOptions> {

    public StoneDBProvider() {
        super(StoneDBGlobalState.class, StoneDBOptions.class);
    }

    public static class StoneDBGlobalState extends SQLGlobalState<StoneDBOptions, StoneDBSchema> {
        @Override
        protected StoneDBSchema readSchema() throws Exception {
            return StoneDBSchema.fromConnection(getConnection(), getDatabaseName());
        }
    }

    @Override
    public void generateDatabase(StoneDBGlobalState globalState) throws Exception {
        return;
    }

    @Override
    public SQLConnection createDatabase(StoneDBGlobalState globalState) throws Exception {
        return null;
    }

    @Override
    public String getDBMSName() {
        return "stonedb";
    }
}
