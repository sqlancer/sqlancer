package sqlancer.yugabyte.ysql;

import java.util.Arrays;
import java.util.List;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import sqlancer.DBMSSpecificOptions;

@Parameters(separators = "=", commandDescription = "YSQL (default port: " + YSQLOptions.DEFAULT_PORT
        + ", default host: " + YSQLOptions.DEFAULT_HOST)
public class YSQLOptions implements DBMSSpecificOptions<YSQLOracleFactory> {
    public static final String DEFAULT_HOST = "localhost";
    public static final int DEFAULT_PORT = 5433;

    @Parameter(names = "--bulk-insert", description = "Specifies whether INSERT statements should be issued in bulk", arity = 1)
    public boolean allowBulkInsert;

    @Parameter(names = "--oracle", description = "Specifies which test oracle should be used for YSQL")
    public List<YSQLOracleFactory> oracle = Arrays.asList(YSQLOracleFactory.QUERY_PARTITIONING);

    @Parameter(names = "--test-collations", description = "Specifies whether to test different collations", arity = 1)
    public boolean testCollations = true;

    @Parameter(names = "--connection-url", description = "Specifies the URL for connecting to the YSQL server", arity = 1)
    public String connectionURL = String.format("jdbc:yugabytedb://%s:%d/yugabyte", YSQLOptions.DEFAULT_HOST,
            YSQLOptions.DEFAULT_PORT);

    @Override
    public List<YSQLOracleFactory> getTestOracleFactory() {
        return oracle;
    }

}
