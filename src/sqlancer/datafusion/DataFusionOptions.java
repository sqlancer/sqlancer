package sqlancer.datafusion;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import sqlancer.DBMSSpecificOptions;
import sqlancer.OracleFactory;
import sqlancer.common.oracle.TestOracle;
import sqlancer.datafusion.DataFusionOptions.DataFusionOracleFactory;
import sqlancer.datafusion.DataFusionProvider.DataFusionGlobalState;
import sqlancer.datafusion.test.DataFusionNoRECOracle;
import sqlancer.datafusion.test.DataFusionQueryPartitioningWhereTester;

@Parameters(commandDescription = "DataFusion")
public class DataFusionOptions implements DBMSSpecificOptions<DataFusionOracleFactory> {
    @Parameter(names = "--debug-info", description = "Show debug messages related to DataFusion", arity = 0)
    public boolean showDebugInfo;

    @Override
    public List<DataFusionOracleFactory> getTestOracleFactory() {
        return Arrays.asList(DataFusionOracleFactory.NOREC, DataFusionOracleFactory.QUERY_PARTITIONING_WHERE);
    }

    public enum DataFusionOracleFactory implements OracleFactory<DataFusionGlobalState> {
        NOREC {
            @Override
            public TestOracle<DataFusionGlobalState> create(DataFusionGlobalState globalState) throws SQLException {
                return new DataFusionNoRECOracle(globalState);
            }
        },
        QUERY_PARTITIONING_WHERE {
            @Override
            public TestOracle<DataFusionGlobalState> create(DataFusionGlobalState globalState) throws SQLException {
                return new DataFusionQueryPartitioningWhereTester(globalState);
            }
        }
    }

}
