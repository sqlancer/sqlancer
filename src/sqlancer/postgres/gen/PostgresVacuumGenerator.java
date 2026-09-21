package sqlancer.postgres.gen;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.PostgresSchema.PostgresTable;

public final class PostgresVacuumGenerator {

    private PostgresVacuumGenerator() {
    }

    public static SQLQueryAdapter create(PostgresGlobalState globalState) {
        PostgresTable table = globalState.getSchema().getRandomTable();
        StringBuilder sb = new StringBuilder("VACUUM ");

        if (Randomly.getBoolean()) {
            sb.append("(");
            java.util.LinkedHashSet<String> seen = new java.util.LinkedHashSet<>();
            boolean sawFull = false;
            boolean sawAnalyze = false;
            boolean sawBuf = false;

            int optsCount = Randomly.smallNumber() + 1;
            for (int i = 0; i < optsCount; i++) {
                // pool of options
                String option = Randomly.fromList(new ArrayList<>(Arrays.asList("FULL", "FREEZE", "ANALYZE", "VERBOSE",
                        "DISABLE_PAGE_SKIPPING", "SKIP_LOCKED", "INDEX_CLEANUP", "TRUNCATE", "BUFFER_USAGE_LIMIT")));

                // skip duplicates
                if (option.equals("BUFFER_USAGE_LIMIT") && sawBuf) {
                    continue;
                }
                if (seen.contains(option) && !option.equals("INDEX_CLEANUP")) {
                    continue;
                }

                // dependencies
                if (option.equals("BUFFER_USAGE_LIMIT")) {
                    sawBuf = true;
                    if (sawFull && !sawAnalyze) {
                        seen.add("ANALYZE");
                        sawAnalyze = true;
                    }
                } else if (option.equals("FULL")) {
                    sawFull = true;
                    if (sawBuf && !sawAnalyze) {
                        seen.add("ANALYZE");
                        sawAnalyze = true;
                    }
                } else if (option.equals("ANALYZE")) {
                    sawAnalyze = true;
                }

                seen.add(option);
            }

            // emit with arguments
            boolean first = true;
            for (String opt : seen) {
                if (!first) {
                    sb.append(", ");
                }
                first = false;
                sb.append(opt);
                if (opt.equals("INDEX_CLEANUP")) {
                    sb.append(" ").append(Randomly.fromOptions("AUTO", "ON", "OFF"));
                } else if (opt.equals("BUFFER_USAGE_LIMIT")) {
                    sb.append(" ");
                    if (Randomly.getBoolean()) {
                        // numeric kB: 0 or [128 .. 16777216]
                        int val = Randomly.fromOptions(0, 128, 256, 512, 1024, 4096, 65536, 262144, 1048576, 16777216);
                        sb.append(val);
                    } else {
                        // quoted with unit
                        String unit = Randomly.fromOptions("kB", "MB", "GB");
                        String size = unit.equals("kB") ? Randomly.fromOptions("128", "256", "512", "1024")
                                : unit.equals("MB") ? Randomly.fromOptions("1", "8", "32", "64", "128", "512")
                                        : /* GB */ Randomly.fromOptions("1", "2", "4", "8", "16");
                        sb.append("'").append(size).append(unit.equals("kB") ? " kB" : unit).append("'");
                    }
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
            } else if (Randomly.getBoolean()) {
                sb.append(" ").append(table.getName());
            }
        }

        ExpectedErrors errors = new ExpectedErrors();
        errors.add("VACUUM cannot run inside a transaction block");
        errors.add("deadlock");
        errors.add("ANALYZE option must be specified when a column list is provided");
        errors.add("VACUUM option DISABLE_PAGE_SKIPPING cannot be used with FULL");
        // The FULL+BUFFER_USAGE_LIMIT check is enforced before execution, so no need to expect it now.
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
