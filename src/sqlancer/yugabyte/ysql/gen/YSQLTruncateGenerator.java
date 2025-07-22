package sqlancer.yugabyte.ysql.gen;

import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.schema.AbstractTable;
import sqlancer.yugabyte.ysql.YSQLErrors;
import sqlancer.yugabyte.ysql.YSQLGlobalState;
import sqlancer.IgnoreMeException;

public final class YSQLTruncateGenerator {

    private YSQLTruncateGenerator() {
    }

    public static SQLQueryAdapter create(YSQLGlobalState globalState) {
        // First get only actual tables (not views)
        List<String> tableNames = globalState.getSchema().getDatabaseTables().stream()
                .filter(t -> !t.isView())
                .map(AbstractTable::getName)
                .collect(Collectors.toList());
        
        // If no tables exist, skip TRUNCATE
        if (tableNames.isEmpty()) {
            throw new IgnoreMeException();
        }
        
        // Get a random non-empty subset of tables
        List<String> selectedTables = Randomly.nonEmptySubset(tableNames);
        
        StringBuilder sb = new StringBuilder();
        sb.append("TRUNCATE");
        if (Randomly.getBoolean()) {
            sb.append(" TABLE");
        }
        sb.append(" ");
        sb.append(String.join(", ", selectedTables));
        
        // TODO remove Restart read required after proper tx ddls
        ExpectedErrors errors = ExpectedErrors
                .from("cannot truncate a table referenced in a foreign key constraint", "is not a table");
        YSQLErrors.addTransactionErrors(errors);
        return new SQLQueryAdapter(sb.toString(), errors);
    }

}
