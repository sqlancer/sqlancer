package sqlancer.postgres.gen;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.PostgresSchema.PostgresTable;

/**
 * Generates VACUUM commands for PostgreSQL.
 * This implementation supports PostgreSQL 13 features including the PARALLEL option.
 * The PARALLEL option specifies the number of parallel worker processes to use when running
 * a parallel vacuum (only applies to the collection of table data, not indexes).
 */
public final class PostgresVacuumGenerator {

    private PostgresVacuumGenerator() {
    }

    public static SQLQueryAdapter create(PostgresGlobalState globalState) {
        PostgresTable table = globalState.getSchema().getRandomTable();
        StringBuilder sb = new StringBuilder("VACUUM ");
        if (Randomly.getBoolean()) {
            // VACUUM [ ( { FULL | FREEZE | VERBOSE | ANALYZE | DISABLE_PAGE_SKIPPING } [,
            // ...] ) ] [ table_name [ (column_name [, ...] ) ] ]
            sb.append("(");
            for (int i = 0; i < Randomly.smallNumber() + 1; i++) {
                ArrayList<String> opts = new ArrayList<>(Arrays.asList("FULL", "FREEZE", "ANALYZE", "VERBOSE",
                        "DISABLE_PAGE_SKIPPING", "SKIP_LOCKED", "INDEX_CLEANUP", "TRUNCATE"));

                // Add PARALLEL option if PostgreSQL version is 13 or higher
                int majorVersion = globalState.getMajorVersion();
                if (majorVersion >= 13) {
                    opts.add("PARALLEL");
                }
                
                String option = Randomly.fromList(opts);
                if (i != 0) {
                    sb.append(", ");
                }
                sb.append(option);
                
                // For PostgreSQL v13+, PARALLEL always needs a value
                if (option.equals("PARALLEL") && majorVersion >= 13) {
                    // PARALLEL takes integer value between 0 and 1024
                    sb.append(" ");
                    sb.append(Randomly.getNotCachedInteger(0, 4)); // Use reasonable value for testing
                } 
                // For other options, randomly decide whether to append a value
                else if (Randomly.getBoolean()) {
                    sb.append(" ");
                    sb.append(Randomly.fromOptions(1, 0));
                }
            }
            sb.append(")");
            if (Randomly.getBoolean()) {
                addTableAndColumns(table, sb);
            }
        } else {
            String firstOption = Randomly.fromOptions("FULL", "FREEZE", "VERBOSE");
            sb.append(firstOption);
            if (Randomly.getBoolean()) {
                sb.append(" ANALYZE");
                addTableAndColumns(table, sb);
            } else {
                if (Randomly.getBoolean()) {
                    sb.append(" ");
                    sb.append(table.getName());
                }
            }
        }
        ExpectedErrors errors = new ExpectedErrors();
        errors.add("VACUUM cannot run inside a transaction block");
        errors.add("deadlock"); /*
                                 * "FULL" commented out due to https://www.postgresql.org/message-id/CA%2Bu7OA6pL%
                                 * 2B7Xm_NXHLenxffe3tCr3gTamVdr7zPjcWqW0RFM-A%40mail.gmail.com
                                 */
        errors.add("ERROR: ANALYZE option must be specified when a column list is provided");
        errors.add("VACUUM option DISABLE_PAGE_SKIPPING cannot be used with FULL");
        
        // Add error specific to PostgreSQL v13's PARALLEL option
        if (globalState.getMajorVersion() >= 13) {
            errors.add("VACUUM option PARALLEL cannot be used with FULL");
        }
        
        return new SQLQueryAdapter(sb.toString(), errors);
    }

    private static void addTableAndColumns(PostgresTable table, StringBuilder sb) {
        sb.append(" ");
        sb.append(table.getName());
        if (Randomly.getBoolean()) {
            sb.append("(");
            sb.append(table.getRandomNonEmptyColumnSubset().stream().map(c -> c.getName())
                    .collect(Collectors.joining(", ")));
            sb.append(")");
        }
    }

}
