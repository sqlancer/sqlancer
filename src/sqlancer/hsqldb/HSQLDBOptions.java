package sqlancer.hsqldb;

import java.sql.SQLException;
import java.util.List;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import sqlancer.DBMSSpecificOptions;
import sqlancer.OracleFactory;
import sqlancer.common.oracle.TestOracle;
import sqlancer.hsqldb.test.HSQLDBNoRECOracle;
import sqlancer.hsqldb.test.HSQLDBQueryPartitioningWhereTester;

@Parameters(commandDescription = "hsqldb")
public class HSQLDBOptions implements DBMSSpecificOptions<HSQLDBOptions.HSQLDBOracleFactory> {

    @Parameter(names = "--oracle")
    public List<HSQLDBOptions.HSQLDBOracleFactory> oracle = List.of(HSQLDBOracleFactory.WHERE,
            HSQLDBOracleFactory.NOREC);

    public enum HSQLDBOracleFactory implements OracleFactory<HSQLDBProvider.HSQLDBGlobalState> {
        WHERE {
            @Override
            public TestOracle create(HSQLDBProvider.HSQLDBGlobalState globalState) throws SQLException {
                return new HSQLDBQueryPartitioningWhereTester(globalState);
            }
        },
        NOREC {
            @Override
            public TestOracle create(HSQLDBProvider.HSQLDBGlobalState globalState) throws Exception {
                return new HSQLDBNoRECOracle(globalState);
            }
        }
    }

    @Override
    public List<HSQLDBOracleFactory> getTestOracleFactory() {
        return oracle;
    }

}
