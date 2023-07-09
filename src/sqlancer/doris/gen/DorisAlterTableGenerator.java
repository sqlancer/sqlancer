package sqlancer.doris.gen;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.doris.DorisProvider.DorisGlobalState;
import sqlancer.doris.DorisSchema.DorisCompositeDataType;
import sqlancer.doris.DorisSchema.DorisTable;

public final class DorisAlterTableGenerator {

    private DorisAlterTableGenerator() {
    }

    enum Action {
        ADD_COLUMN, ALTER_COLUMN, DROP_COLUMN
    }

    public static SQLQueryAdapter getQuery(DorisGlobalState globalState) {
        ExpectedErrors errors = new ExpectedErrors();
        StringBuilder sb = new StringBuilder("ALTER TABLE ");
        DorisTable table = globalState.getSchema().getRandomTable(t -> !t.isView());
        sb.append(table.getName());
        sb.append(" ");
        Action action = Randomly.fromOptions(Action.values());
        switch (action) {
        case ADD_COLUMN:
            sb.append("ADD COLUMN ");
            String columnName = table.getFreeColumnName();
            sb.append(columnName);
            sb.append(" ");
            sb.append(DorisCompositeDataType.getRandomWithoutNull(globalState).toString());
            break;
        case ALTER_COLUMN:
            sb.append("MODIFY COLUMN ");
            sb.append(table.getRandomColumn().getName());
            sb.append(" ");
            sb.append(DorisCompositeDataType.getRandomWithoutNull(globalState).toString());
            break;
        case DROP_COLUMN:
            sb.append("DROP COLUMN ");
            sb.append(table.getRandomColumn().getName());
            break;
        default:
            throw new AssertionError(action);
        }
        return new SQLQueryAdapter(sb.toString(), errors, true);
    }

}
