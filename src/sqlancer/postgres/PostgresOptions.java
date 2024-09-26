package sqlancer.postgres;

import java.util.Arrays;
import java.util.List;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import sqlancer.DBMSSpecificOptions;

@Parameters(separators = "=", commandDescription = "PostgreSQL (default port: " + PostgresOptions.DEFAULT_PORT
        + ", default host: " + PostgresOptions.DEFAULT_HOST + ")")
public class PostgresOptions implements DBMSSpecificOptions<PostgresOracleFactory> {
    public static final String DEFAULT_HOST = "localhost";
    public static final int DEFAULT_PORT = 5432;

    @Parameter(names = "--bulk-insert", description = "Specifies whether INSERT statements should be issued in bulk", arity = 1)
    public boolean allowBulkInsert;

    @Parameter(names = "--oracle", description = "Specifies which test oracle should be used for PostgreSQL")
    public List<PostgresOracleFactory> oracle = Arrays.asList(PostgresOracleFactory.QUERY_PARTITIONING);

    @Parameter(names = "--test-collations", description = "Specifies whether to test different collations", arity = 1)
    public boolean testCollations = true;

    @Parameter(names = "--connection-url", description = "Specifies the URL for connecting to the PostgreSQL server", arity = 1)
    public String connectionURL = String.format("postgresql://%s:%d/test", PostgresOptions.DEFAULT_HOST,
            PostgresOptions.DEFAULT_PORT);

    @Parameter(names = "--extensions", description = "Specifies a comma-separated list of extension names to be created in each test database", arity = 1)
    public String extensions = "";

    @Override
    public List<PostgresOracleFactory> getTestOracleFactory() {
        return oracle;
    }

}
