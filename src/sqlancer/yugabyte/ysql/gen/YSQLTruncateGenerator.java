package sqlancer.yugabyte.ysql.gen;

import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.schema.AbstractTable;
import sqlancer.yugabyte.ysql.YSQLGlobalState;

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
        sb.append(globalState.getSchema().getDatabaseTablesRandomSubsetNotEmpty().stream().map(AbstractTable::getName)
                .collect(Collectors.joining(", ")));
        return new SQLQueryAdapter(sb.toString(), ExpectedErrors
                .from("cannot truncate a table referenced in a foreign key constraint", "is not a table"));
    }

}
