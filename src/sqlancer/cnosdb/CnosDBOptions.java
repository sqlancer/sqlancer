package sqlancer.cnosdb;

import java.util.List;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import sqlancer.DBMSSpecificOptions;

@Parameters(separators = "=", commandDescription = "CnosDB (default port: " + CnosDBOptions.DEFAULT_PORT
        + ", default host: " + CnosDBOptions.DEFAULT_HOST + ")")
public class CnosDBOptions implements DBMSSpecificOptions<CnosDBOracleFactory> {

    public static final String DEFAULT_HOST = "localhost";
    public static final int DEFAULT_PORT = 31001;

    @Parameter(names = "--oracle", description = "Specifies which test oracle should be used for CnosDB")
    public List<CnosDBOracleFactory> oracle = List.of(CnosDBOracleFactory.QUERY_PARTITIONING);

    @Parameter(names = "--connection-url", description = "Specifies the URL for connecting to the CnosDB", arity = 1)
    public String connectionURL = String.format("http://%s:%d", CnosDBOptions.DEFAULT_HOST, CnosDBOptions.DEFAULT_PORT);

    @Override
    public List<CnosDBOracleFactory> getTestOracleFactory() {
        return oracle;
    }

}
