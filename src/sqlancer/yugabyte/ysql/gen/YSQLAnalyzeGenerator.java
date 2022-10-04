package sqlancer.yugabyte.ysql.gen;

import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.schema.AbstractTableColumn;
import sqlancer.yugabyte.ysql.YSQLGlobalState;
import sqlancer.yugabyte.ysql.YSQLSchema.YSQLTable;

public final class YSQLAnalyzeGenerator {

    private YSQLAnalyzeGenerator() {
    }

    public static SQLQueryAdapter create(YSQLGlobalState globalState) {
        YSQLTable table = globalState.getSchema().getRandomTable();
        StringBuilder sb = new StringBuilder("ANALYZE");
        if (Randomly.getBoolean()) {
            sb.append("(");
            sb.append(" VERBOSE");
            sb.append(")");
        }
        sb.append(" ");
        sb.append(table.getName());
        if (Randomly.getBoolean()) {
            sb.append("(");
            sb.append(table.getRandomNonEmptyColumnSubset().stream().map(AbstractTableColumn::getName)
                    .collect(Collectors.joining(", ")));
            sb.append(")");
        }

        return new SQLQueryAdapter(sb.toString(), ExpectedErrors.from("deadlock"));
    }

}
