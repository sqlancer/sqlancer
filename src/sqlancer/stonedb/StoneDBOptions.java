package sqlancer.stonedb;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import sqlancer.DBMSSpecificOptions;
import sqlancer.OracleFactory;
import sqlancer.common.oracle.TestOracle;
import sqlancer.mysql.MySQLOptions;
import sqlancer.stonedb.StoneDBProvider.StoneDBGlobalState;
import sqlancer.stonedb.oracle.StoneDBNoRECOracle;
import sqlancer.stonedb.oracle.StoneDBTLPOracle;
import sqlancer.stonedb.StoneDBOptions.StoneDBOracleFactory;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

@Parameters(separators = "=", commandDescription = "StoneDB (default host: " + MySQLOptions.DEFAULT_HOST
        + ", default port: " + MySQLOptions.DEFAULT_PORT + ")")
public class StoneDBOptions implements DBMSSpecificOptions<StoneDBOracleFactory> {
    public static final String DEFAULT_HOST = "localhost";
    public static final int DEFAULT_PORT = 3306;

    @Parameter(names = "--oracle")
    public List<StoneDBOracleFactory> oracles = Arrays.asList(StoneDBOracleFactory.NOREC);

    public enum StoneDBOracleFactory implements OracleFactory<StoneDBGlobalState> {
        NOREC {
            @Override
            public TestOracle<StoneDBGlobalState> create(StoneDBGlobalState globalState) throws SQLException {
                return new StoneDBNoRECOracle(globalState);
            }
        },
        TLP {
            @Override
            public TestOracle<StoneDBGlobalState> create(StoneDBGlobalState globalState) throws Exception {
                return new StoneDBTLPOracle(globalState);
            }
        }
    }

    @Override
    public List<StoneDBOracleFactory> getTestOracleFactory() {
        return oracles;
    }
}
