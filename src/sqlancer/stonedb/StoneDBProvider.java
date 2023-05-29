package sqlancer.stonedb;

import sqlancer.SQLGlobalState;
import sqlancer.mysql.MySQLGlobalState;
import sqlancer.mysql.MySQLProvider;

import java.sql.SQLException;

public class StoneDBProvider extends MySQLProvider {
    public static class StoneDBGlobalState extends MySQLGlobalState {
    }
}
