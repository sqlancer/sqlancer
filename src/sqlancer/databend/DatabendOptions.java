package sqlancer.databend;

import java.util.Arrays;
import java.util.List;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import sqlancer.SQLOptions;

@Parameters(commandDescription = "Databend")
public class DatabendOptions extends SQLOptions<DatabendOracleFactory> {

    @Parameter(names = "--oracle")
    public List<DatabendOracleFactory> oracles = Arrays.asList(DatabendOracleFactory.QUERY_PARTITIONING);

    @Override
    public List<DatabendOracleFactory> getTestOracleFactory() {
        return oracles;
    }

}
