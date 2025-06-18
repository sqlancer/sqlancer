package sqlancer.postgres;

import java.io.File;
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
    private static Boolean defaultTestTablespaces;

    @Parameter(names = "--bulk-insert", description = "Specifies whether INSERT statements should be issued in bulk", arity = 1)
    public boolean allowBulkInsert;

    @Parameter(names = "--oracle", description = "Specifies which test oracle should be used for PostgreSQL")
    public List<PostgresOracleFactory> oracle = Arrays.asList(PostgresOracleFactory.QUERY_PARTITIONING);

    @Parameter(names = "--connection-timeout", description = "Timeout in seconds for connecting to the server", arity = 1)
    public int connectionTimeoutInSeconds;

    @Parameter(names = "--test-collations", description = "Specifies whether to test different collations", arity = 1)
    public boolean testCollations = true;

    @Parameter(names = "--test-tablespaces", description = "Specifies whether to test tablespace creation (default is OS-dependent)", arity = 1)
    public boolean testTablespaces;

    @Parameter(names = "--tablespace-path", description = "Base path for tablespace directories (default is OS-dependent)", arity = 1)
    public String tablespacePath = getDefaultTablespacePath();

    @Parameter(names = "--connection-url", description = "Specifies the URL for connecting to the PostgreSQL server", arity = 1)
    public String connectionURL = String.format("postgresql://%s:%d/test", PostgresOptions.DEFAULT_HOST,
            PostgresOptions.DEFAULT_PORT);

    @Parameter(names = "--extensions", description = "Specifies a comma-separated list of extension names to be created in each test database", arity = 1)
    public String extensions = "";

    private static boolean determineDefaultTablespaceSupport() {
        String osName = System.getProperty("os.name").toLowerCase();
        if (osName.contains("linux")) {
            System.out.println("[INFO] Linux detected: Enabling tablespace testing by default");
            return true;
        } else if (osName.contains("mac") || osName.contains("darwin")) {
            System.out.println(
                    "[INFO] macOS detected: Disabling tablespace testing by default due to different /tmp handling. Override with --test-tablespaces=true and ensure proper directory permissions.");
            return false;
        } else if (osName.contains("windows")) {
            System.out.println(
                    "[INFO] Windows detected: Disabling tablespace testing by default due to path format differences. Override with --test-tablespaces=true and use --tablespace-path to set a valid Windows path.");
            return false;
        } else {
            System.out.println(
                    "[INFO] Unknown OS detected: Disabling tablespace testing by default for safety. Override with --test-tablespaces=true if your system supports PostgreSQL tablespaces.");
            return false;
        }
    }

    public static String getDefaultTablespacePath() {
        String osName = System.getProperty("os.name").toLowerCase();
        if (osName.contains("windows")) {
            // On Windows, use a path in the temp directory
            return new File(System.getProperty("java.io.tmpdir"), "postgresql" + File.separator + "tablespace")
                    .getAbsolutePath();
        } else {
            // On Unix-like systems, use /tmp
            return "/tmp/postgresql/tablespace";
        }
    }

    @Override
    public List<PostgresOracleFactory> getTestOracleFactory() {
        return oracle;
    }

    public String getTablespacePath() {
        if (tablespacePath == null || tablespacePath.isBlank()) {
            throw new AssertionError("Tablespace path is null or empty. Please configure --tablespace-path");
        }

        File path = new File(tablespacePath);

        // Check if the directory exists or can be created
        if (!path.exists() && !path.mkdirs()) {
            throw new AssertionError("Cannot create tablespace directory: " + tablespacePath
                    + ". Please ensure the parent directory exists and you have write permissions.");
        }

        // Check if it's actually a directory
        if (!path.isDirectory()) {
            throw new AssertionError("Tablespace path is not a directory: " + tablespacePath);
        }

        // Check write permissions
        if (!path.canWrite()) {
            throw new AssertionError("No write permissions for tablespace directory: " + tablespacePath
                    + ". Please ensure you have write permissions to this directory.");
        }

        return tablespacePath;
    }

    public boolean isTestTablespaces() {
        // If the user explicitly set the value via command line, use that
        // Otherwise, use the OS-dependent default
        return testTablespaces || getDefaultTablespaceSupport();
    }

    private static boolean getDefaultTablespaceSupport() {
        if (defaultTestTablespaces == null) {
            defaultTestTablespaces = determineDefaultTablespaceSupport();
        }
        return defaultTestTablespaces;
    }
}
