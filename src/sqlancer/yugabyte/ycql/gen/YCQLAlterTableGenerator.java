package sqlancer.yugabyte.ycql.gen;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.yugabyte.ycql.YCQLProvider.YCQLGlobalState;
import sqlancer.yugabyte.ycql.YCQLSchema.YCQLCompositeDataType;
import sqlancer.yugabyte.ycql.YCQLSchema.YCQLTable;

public final class YCQLAlterTableGenerator {

    private YCQLAlterTableGenerator() {
    }

    enum Action {
        ADD_COLUMN, DROP_COLUMN
    }

    public static SQLQueryAdapter getQuery(YCQLGlobalState globalState) {
        ExpectedErrors errors = new ExpectedErrors();
        StringBuilder sb = new StringBuilder("ALTER TABLE ");
        YCQLTable table = globalState.getSchema().getRandomTable(t -> !t.isView());
        sb.append(table.getName());
        sb.append(" ");
        Action action = Randomly.fromOptions(Action.values());
        switch (action) {
        case ADD_COLUMN:
            sb.append("ADD ");
            String columnName = table.getFreeColumnName();
            sb.append(columnName);
            sb.append(" ");
            sb.append(YCQLCompositeDataType.getRandom().toString());
            break;
        case DROP_COLUMN:
            sb.append("DROP ");
            sb.append(table.getRandomColumn().getName());
            break;
        default:
            throw new AssertionError(action);
        }

        errors.add("Alter key column. Can't alter key column");
        errors.add("cannot remove a key column");

        return new SQLQueryAdapter(sb.toString(), errors, true);
    }

}
