package sqlancer.hsqldb;

import java.sql.SQLException;
import java.util.List;

import com.beust.jcommander.Parameter;

import sqlancer.DBMSSpecificOptions;
import sqlancer.OracleFactory;
import sqlancer.common.oracle.TestOracle;
import sqlancer.hsqldb.test.HSQLDBQueryPartitioningWhereTester;

public class HSQLDBOptions implements DBMSSpecificOptions<HSQLDBOptions.HSQLDBOracleFactory> {

    @Parameter(names = "--oracle")
    public List<HSQLDBOptions.HSQLDBOracleFactory> oracle = List.of(HSQLDBOracleFactory.WHERE);

    public enum HSQLDBOracleFactory implements OracleFactory<HSQLDBProvider.HSQLDBGlobalState> {
        WHERE {
            @Override
            public TestOracle create(HSQLDBProvider.HSQLDBGlobalState globalState) throws SQLException {
                return new HSQLDBQueryPartitioningWhereTester(globalState);
            }
        }
    }

    @Override
    public List<HSQLDBOracleFactory> getTestOracleFactory() {
        return oracle;
    }

}
