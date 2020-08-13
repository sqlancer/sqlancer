package sqlancer.mysql;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import sqlancer.DBMSSpecificOptions;
import sqlancer.OracleFactory;
import sqlancer.common.oracle.TestOracle;
import sqlancer.mysql.MySQLOptions.MySQLOracleFactory;
import sqlancer.mysql.oracle.MySQLTLPWhereOracle;

@Parameters
public class MySQLOptions implements DBMSSpecificOptions<MySQLOracleFactory> {

    @Parameter(names = "--oracle")
    public List<MySQLOracleFactory> oracles = Arrays.asList(MySQLOracleFactory.TLP_WHERE);

    public enum MySQLOracleFactory implements OracleFactory<MySQLGlobalState> {

        TLP_WHERE {

            @Override
            public TestOracle create(MySQLGlobalState globalState) throws SQLException {
                return new MySQLTLPWhereOracle(globalState);
            }

        }
    }

    @Override
    public List<MySQLOracleFactory> getTestOracleFactory() {
        return oracles;
    }

}
