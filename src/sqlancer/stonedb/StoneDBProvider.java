package sqlancer.stonedb;

import com.google.auto.service.AutoService;
import sqlancer.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

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
        // todo
        return;
    }

    @Override
    public SQLConnection createDatabase(StoneDBGlobalState globalState) throws Exception {
        String username = globalState.getOptions().getUserName();
        String password = globalState.getOptions().getPassword();
        String host = globalState.getOptions().getHost();
        int port = globalState.getOptions().getPort();
        if (host == null) {
            host = StoneDBOptions.DEFAULT_HOST;
        }
        if (port == MainOptions.NO_SET_PORT) {
            port = StoneDBOptions.DEFAULT_PORT;
        }
        String databaseName = globalState.getDatabaseName();
        globalState.getState().logStatement("DROP DATABASE IF EXISTS " + databaseName);
        globalState.getState().logStatement("CREATE DATABASE " + databaseName);
        globalState.getState().logStatement("USE " + databaseName);
        String url = String.format("jdbc:mysql://%s:%d?serverTimezone=UTC&useSSL=false&allowPublicKeyRetrieval=true",
                host, port);
        Connection con = DriverManager.getConnection(url, username, password);
        try (Statement s = con.createStatement()) {
            s.execute("DROP DATABASE IF EXISTS " + databaseName);
        }
        try (Statement s = con.createStatement()) {
            s.execute("CREATE DATABASE " + databaseName);
        }
        try (Statement s = con.createStatement()) {
            s.execute("USE " + databaseName);
        }
        return new SQLConnection(con);
    }

    @Override
    public String getDBMSName() {
        return "stonedb";
    }
}
