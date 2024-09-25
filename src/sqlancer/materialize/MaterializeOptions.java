package sqlancer.materialize;

import java.util.Arrays;
import java.util.List;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import sqlancer.DBMSSpecificOptions;

@Parameters(separators = "=", commandDescription = "Materialize (default port: " + MaterializeOptions.DEFAULT_PORT
        + ", default host: " + MaterializeOptions.DEFAULT_HOST + ", default user: " + MaterializeOptions.DEFAULT_USER
        + ")")
public class MaterializeOptions implements DBMSSpecificOptions<MaterializeOracleFactory> {
    public static final String DEFAULT_HOST = "localhost";
    public static final String DEFAULT_USER = "materialize";
    public static final int DEFAULT_PORT = 6875;

    @Parameter(names = "--bulk-insert", description = "Specifies whether INSERT statements should be issued in bulk", arity = 1)
    public boolean allowBulkInsert;

    @Parameter(names = "--oracle", description = "Specifies which test oracle should be used for Materialize")
    public List<MaterializeOracleFactory> oracle = Arrays.asList(MaterializeOracleFactory.QUERY_PARTITIONING);

    @Parameter(names = "--test-collations", description = "Specifies whether to test different collations", arity = 1)
    public boolean testCollations = true;

    @Parameter(names = "--set-max-tables-mvs", description = "Specifies whether to set the maximum number of tables and materialized views intiially", arity = 1)
    public boolean setMaxTablesMVs;

    @Parameter(names = "--connection-url", description = "Specifies the URL for connecting to the Materialize server", arity = 1)
    public String connectionURL = String.format("postgresql://%s@%s:%d/test", MaterializeOptions.DEFAULT_USER,
            MaterializeOptions.DEFAULT_HOST, MaterializeOptions.DEFAULT_PORT);

    @Parameter(names = "--extensions", description = "Specifies a comma-separated list of extension names to be created in each test database", arity = 1)
    public String extensions = "";

    @Override
    public List<MaterializeOracleFactory> getTestOracleFactory() {
        return oracle;
    }

}
