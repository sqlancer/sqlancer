package sqlancer.yugabyte.ycql;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import sqlancer.DBMSSpecificOptions;
import sqlancer.OracleFactory;
import sqlancer.common.oracle.TestOracle;
import sqlancer.yugabyte.ycql.YCQLOptions.YCQLOracleFactory;
import sqlancer.yugabyte.ycql.YCQLProvider.YCQLGlobalState;
import sqlancer.yugabyte.ycql.test.YCQLFuzzer;

@Parameters(commandDescription = "YCQL")
public class YCQLOptions implements DBMSSpecificOptions<YCQLOracleFactory> {

    @Parameter(names = "--max-num-deletes", description = "The maximum number of DELETE statements that are issued for a database", arity = 1)
    public int maxNumDeletes = 1;

    @Parameter(names = "--max-num-updates", description = "The maximum number of UPDATE statements that are issued for a database", arity = 1)
    public int maxNumUpdates = 5;

    @Parameter(names = "--datacenter", description = "YCQL datacenter, can be found in system.local table", arity = 1)
    public String datacenter = "datacenter1";

    @Parameter(names = "--oracle")
    public List<YCQLOracleFactory> oracles = Arrays.asList(YCQLOracleFactory.FUZZER);

    public enum YCQLOracleFactory implements OracleFactory<YCQLGlobalState> {
        FUZZER {
            @Override
            public TestOracle create(YCQLGlobalState globalState) throws SQLException {
                return new YCQLFuzzer(globalState);
            }

        }
    }

    @Override
    public List<YCQLOracleFactory> getTestOracleFactory() {
        return oracles;
    }

}
