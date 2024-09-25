package sqlancer.oceanbase;

import java.util.Arrays;
import java.util.List;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import sqlancer.DBMSSpecificOptions;

@Parameters(separators = "=", commandDescription = "OceanBase (default port: " + OceanBaseOptions.DEFAULT_PORT
        + ", default host: " + OceanBaseOptions.DEFAULT_HOST + ")")
public class OceanBaseOptions implements DBMSSpecificOptions<OceanBaseOracleFactory> {
    public static final String DEFAULT_HOST = "localhost";
    public static final int DEFAULT_PORT = 2881;

    @Parameter(names = "--oracle")
    public List<OceanBaseOracleFactory> oracles = Arrays.asList(OceanBaseOracleFactory.TLP_WHERE);

    @Parameter(names = { "--query-timeout" }, description = "Query timeout")
    public int queryTimeout = 1000000000;
    @Parameter(names = { "--transaction-timeout" }, description = "Transaction timeout")
    public int trxTimeout = 1000000000;

    @Override
    public List<OceanBaseOracleFactory> getTestOracleFactory() {
        return oracles;
    }

}
