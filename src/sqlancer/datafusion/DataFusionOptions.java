package sqlancer.datafusion;

import java.util.Arrays;
import java.util.List;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import sqlancer.DBMSSpecificOptions;

@Parameters(commandDescription = "DataFusion")
public class DataFusionOptions implements DBMSSpecificOptions<DataFusionOracleFactory> {
    @Parameter(names = "--debug-info", description = "Show debug messages related to DataFusion", arity = 0)
    public boolean showDebugInfo;

    @Override
    public List<DataFusionOracleFactory> getTestOracleFactory() {
        return Arrays.asList(DataFusionOracleFactory.NOREC, DataFusionOracleFactory.QUERY_PARTITIONING_WHERE);
    }

}
