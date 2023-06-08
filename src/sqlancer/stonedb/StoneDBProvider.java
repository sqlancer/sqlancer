package sqlancer.stonedb;

import sqlancer.SQLGlobalState;
import sqlancer.mysql.MySQLGlobalState;
import sqlancer.mysql.MySQLOptions;
import sqlancer.mysql.MySQLProvider;

import java.sql.SQLException;

public class StoneDBProvider extends MySQLProvider {

    public StoneDBProvider() {
        super();
    }

    public static class StoneDBGlobalState extends MySQLGlobalState {
        @Override
        public boolean usesPQS() {
            return false;
        }
    }

    @Override
    public String getDBMSName() {
        return "stonedb";
    }
}
