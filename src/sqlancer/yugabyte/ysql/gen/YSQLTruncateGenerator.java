package sqlancer.yugabyte.ysql.gen;

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
        StringBuilder sb = new StringBuilder();
        sb.append("TRUNCATE");
        if (Randomly.getBoolean()) {
            sb.append(" TABLE");
        }
        sb.append(" ");
        String tableNames = globalState.getSchema().getDatabaseTablesRandomSubsetNotEmpty().stream()
                .filter(t -> !t.isView())  // Exclude views from TRUNCATE
                .map(AbstractTable::getName)
                .collect(Collectors.joining(", "));
        
        // If all selected tables were views, we have nothing to truncate
        if (tableNames.isEmpty()) {
            throw new IgnoreMeException();
        }
        
        sb.append(tableNames);
        // TODO remove Restart read required after proper tx ddls
        ExpectedErrors errors = ExpectedErrors
                .from("cannot truncate a table referenced in a foreign key constraint", "is not a table");
        YSQLErrors.addTransactionErrors(errors);
        return new SQLQueryAdapter(sb.toString(), errors);
    }

}
