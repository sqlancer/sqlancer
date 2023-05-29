package sqlancer.stonedb;

import com.google.auto.service.AutoService;
import sqlancer.*;
import sqlancer.common.DBMSCommon;
import sqlancer.common.schema.AbstractRelationalTable;
import sqlancer.common.schema.TableIndex;
import sqlancer.mysql.MySQLGlobalState;
import sqlancer.mysql.MySQLOptions;
import sqlancer.mysql.MySQLProvider;
import sqlancer.mysql.MySQLSchema;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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
