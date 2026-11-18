package sqlancer.risingwave;

import java.util.Collections;
import java.util.List;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import sqlancer.DBMSSpecificOptions;

@Parameters(separators = "=", commandDescription = "RisingWave (PostgreSQL protocol, default port 4566)")
public class RisingWaveOptions implements DBMSSpecificOptions<RisingWaveOracleFactory> {
    @Parameter(names = "--oracle", description = "Oracle to use")
    public List<RisingWaveOracleFactory> oracle = Collections.singletonList(RisingWaveOracleFactory.WHERE);

    @Parameter(names = "--entry-database", description = "Existing database used only to create isolated test databases")
    public String entryDatabase = "dev";

    @Parameter(names = "--max-relation-depth", description = "Maximum relation recursion depth")
    public int relationDepth = 2;

    @Parameter(names = "--test-time-windows", description = "Generate HOP/TUMBLE relations (RisingWave 3.1)", arity = 1)
    public boolean timeWindows = true;

    @Parameter(names = "--min-tables", description = "Minimum base tables (1..6)")
    public int minTables = 4;
    @Parameter(names = "--max-tables", description = "Maximum base tables (1..6)")
    public int maxTables = 6;
    @Parameter(names = "--min-columns", description = "Minimum columns per table, including join key")
    public int minColumns = 2;
    @Parameter(names = "--max-columns", description = "Maximum columns per table (up to 12)")
    public int maxColumns = 9;
    @Parameter(names = "--min-rows", description = "Minimum rows per table, including edge cases")
    public int minRows;
    @Parameter(names = "--max-rows", description = "Maximum rows per table (up to 100)")
    public int maxRows = 12;
    @Parameter(names = "--max-views", description = "Maximum ordinary views (0..3)")
    public int maxViews = 1;
    @Parameter(names = "--max-materialized-views", description = "Maximum materialized views (0..3)")
    public int maxMaterializedViews = 1;
    @Parameter(names = "--max-mutations", description = "Maximum UPDATE/DELETE setup actions (0..12)")
    public int maxMutations = 4;
    @Parameter(names = "--expression-nodes", description = "Budget for expanding expressions during query generation")
    public int expressionNodes = 160;
    @Parameter(names = "--relation-nodes", description = "Relation construction budget per comparison")
    public int relationNodes = 10;
    @Parameter(names = "--max-window-expansion", description = "Maximum HOP size/slide ratio (1..4)")
    public int maxWindowExpansion = 3;
    @Parameter(names = "--connect-timeout-seconds", description = "JDBC connection timeout")
    public int connectTimeoutSeconds = 10;
    @Parameter(names = "--statement-timeout-seconds", description = "Server and JDBC statement timeout")
    public int statementTimeoutSeconds = 15;
    @Parameter(names = "--retain-databases", description = "Database retention: NONE, FAILURES, ALL")
    public Retention retention = Retention.FAILURES;

    public enum Retention {
        NONE, FAILURES, ALL
    }

    public void validate() {
        if (relationDepth < 0) {
            throw new IllegalArgumentException("RisingWave relation depth must be nonnegative");
        }
        if (maxViews < 0 || maxViews > 3 || maxMaterializedViews < 0 || maxMaterializedViews > 3 || maxMutations < 0
                || maxMutations > 12 || minTables < 1 || minTables > maxTables || maxTables > 6 || minColumns < 1
                || minColumns > maxColumns || maxColumns > 12 || minRows < 0 || minRows > maxRows || maxRows > 100
                || expressionNodes < 1 || expressionNodes > 500 || relationNodes < 1 || relationNodes > 20
                || maxWindowExpansion < 1 || maxWindowExpansion > 4 || connectTimeoutSeconds < 1
                || connectTimeoutSeconds > 120 || statementTimeoutSeconds < 1 || statementTimeoutSeconds > 300) {
            throw new IllegalArgumentException("Invalid RisingWave size/timeout bounds");
        }
    }

    @Override
    public List<RisingWaveOracleFactory> getTestOracleFactory() {
        return oracle;
    }
}
