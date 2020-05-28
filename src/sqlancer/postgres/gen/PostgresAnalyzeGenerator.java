package sqlancer.postgres.gen;

import java.util.Arrays;
import java.util.stream.Collectors;

import sqlancer.Query;
import sqlancer.QueryAdapter;
import sqlancer.Randomly;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.PostgresSchema.PostgresTable;

public final class PostgresAnalyzeGenerator {

    private PostgresAnalyzeGenerator() {
    }

    public static Query create(PostgresGlobalState globalState) {
        PostgresTable table = globalState.getSchema().getRandomTable();
        StringBuilder sb = new StringBuilder("ANALYZE");
        if (Randomly.getBoolean()) {
            sb.append("(");
            if (Randomly.getBoolean()) {
                sb.append(" VERBOSE");
            } else {
                sb.append(" SKIP_LOCKED");
            }
            sb.append(")");
        }
        if (Randomly.getBoolean()) {
            sb.append(" ");
            sb.append(table.getName());
            if (Randomly.getBoolean()) {
                sb.append("(");
                sb.append(table.getRandomNonEmptyColumnSubset().stream().map(c -> c.getName())
                        .collect(Collectors.joining(", ")));
                sb.append(")");
            }
        }
        // FIXME: bug in postgres?
        return new QueryAdapter(sb.toString(), Arrays.asList("deadlock"));
    }

}
