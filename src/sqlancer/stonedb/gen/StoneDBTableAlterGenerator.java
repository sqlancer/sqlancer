package sqlancer.stonedb.gen;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.stonedb.StoneDBProvider.StoneDBGlobalState;
import sqlancer.stonedb.StoneDBSchema.StoneDBCompositeDataType;
import sqlancer.stonedb.StoneDBSchema.StoneDBTable;

public class StoneDBTableAlterGenerator {
    private final StoneDBGlobalState globalState;
    private final StringBuilder sb = new StringBuilder();
    ExpectedErrors errors = new ExpectedErrors();

    enum Action {
        ADD_COLUMN, ALTER_COLUMN, DROP_COLUMN, CHANGE_COLUMN, RENAME_COLUMN
    }

    public StoneDBTableAlterGenerator(StoneDBGlobalState globalState) {
        this.globalState = globalState;
    }

    public static SQLQueryAdapter generate(StoneDBGlobalState globalState) {
        return new StoneDBTableAlterGenerator(globalState).getQuery();
    }

    private SQLQueryAdapter getQuery() {
        sb.append("ALTER TABLE ");
        StoneDBTable table = globalState.getSchema().getRandomTable(t -> !t.isView());
        StoneDBExpressionGenerator generator = new StoneDBExpressionGenerator(globalState)
                .setColumns(table.getColumns());
        sb.append(table.getName());
        sb.append(" ");
        Action action = Randomly.fromOptions(Action.values());
        switch (action) {
        case ADD_COLUMN:
            sb.append("ADD COLUMN ");
            String columnName = table.getFreeColumnName();
            sb.append(columnName);
            sb.append(" ");
            sb.append(StoneDBCompositeDataType.getRandomWithoutNull());
            if (Randomly.getBoolean()) {
                if (Randomly.getBoolean()) {
                    sb.append(" FIRST");
                } else {
                    sb.append(" AFTER ");
                    sb.append(table.getRandomColumn().getName());
                }
            }
            break;
        case DROP_COLUMN:
            sb.append(Randomly.fromOptions("DROP COLUMN ", "DROP "));
            sb.append(table.getRandomColumn().getName());
            break;
        case ALTER_COLUMN:
            sb.append(Randomly.fromOptions("ALTER COLUMN ", "ALTER "));
            sb.append(table.getRandomColumn().getName());
            sb.append("{");
            if (Randomly.getBoolean()) {
                sb.append(" SET DEFAULT ").append(generator.generateExpression());
            } else {
                sb.append(" DROP DEFAULT");
            }
            if (Randomly.getBoolean()) {
                sb.append(" SET ").append(Randomly.fromOptions("VISIBLE", "INVISIBLE"));
            }
            sb.append("}");
            break;
        case CHANGE_COLUMN:
            sb.append(Randomly.fromOptions("CHANGE COLUMN ", "CHANGE "));
            String oldColumnName = table.getRandomColumn().getName();
            String newColumnName = table.getFreeColumnName();
            sb.append(oldColumnName).append(" ").append(newColumnName);
            sb.append(" ");
            sb.append(StoneDBCompositeDataType.getRandomWithoutNull());
            if (Randomly.getBoolean()) {
                if (Randomly.getBoolean()) {
                    sb.append(" FIRST");
                } else {
                    sb.append(" AFTER ");
                    sb.append(table.getRandomColumn().getName());
                }
            }
            break;
        case RENAME_COLUMN:
            sb.append("RENAME COLUMN ");
            sb.append(table.getRandomColumn().getName());
            sb.append(" TO ");
            sb.append(table.getFreeColumnName());
            break;
        default:
            throw new AssertionError(action);
        }
        return new SQLQueryAdapter(sb.toString(), errors, true);
    }
}
