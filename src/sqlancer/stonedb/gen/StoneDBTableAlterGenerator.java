package sqlancer.stonedb.gen;

import java.util.List;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.stonedb.StoneDBProvider.StoneDBGlobalState;
import sqlancer.stonedb.StoneDBSchema.StoneDBColumn;
import sqlancer.stonedb.StoneDBSchema.StoneDBCompositeDataType;
import sqlancer.stonedb.StoneDBSchema.StoneDBDataType;
import sqlancer.stonedb.StoneDBSchema.StoneDBTable;

public class StoneDBTableAlterGenerator {
    private final StoneDBGlobalState globalState;
    private final StringBuilder sb = new StringBuilder();
    private final StoneDBTable table;
    ExpectedErrors errors = new ExpectedErrors();

    enum Action {
        ADD_COLUMN, ALTER_COLUMN, DROP_COLUMN, CHANGE_COLUMN, RENAME_COLUMN
    }

    public StoneDBTableAlterGenerator(StoneDBGlobalState globalState) {
        this.globalState = globalState;
        table = globalState.getSchema().getRandomTable(t -> !t.isView());
    }

    public static SQLQueryAdapter generate(StoneDBGlobalState globalState) {
        return new StoneDBTableAlterGenerator(globalState).getQuery();
    }

    private SQLQueryAdapter getQuery() {
        sb.append("ALTER TABLE ");
        sb.append(table.getName());
        sb.append(" ");
        appendAlterOptions();
        addExpectedErrors();
        return new SQLQueryAdapter(sb.toString(), errors, true);
    }

    private void addExpectedErrors() {
        // java.sql.SQLSyntaxErrorException: You can't delete all columns with ALTER TABLE; use DROP TABLE instead
        errors.add("You can't delete all columns with ALTER TABLE; use DROP TABLE instead");
    }

    private void appendAlterOptions() {
        List<Action> actions;
        if (Randomly.getBooleanWithSmallProbability()) {
            actions = Randomly.subset(Action.values());
        } else {
            actions = List.of(Randomly.fromOptions(Action.values()));
        }
        for (Action action : actions) {
            appendAlterOption(action);
        }
    }

    private void appendAlterOption(Action action) {
        StoneDBExpressionGenerator generator = new StoneDBExpressionGenerator(globalState)
                .setColumns(table.getColumns());
        switch (action) {
        case ADD_COLUMN:
            sb.append("ADD COLUMN ");
            String columnName = table.getFreeColumnName();
            sb.append(columnName);
            sb.append(" ");
            sb.append(StoneDBCompositeDataType.getRandomWithoutNull().getPrimitiveDataType().toString());
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
            sb.append(Randomly.fromOptions("CHANGE COLUMN ", "CHANGE "));
            StoneDBColumn randomColumn = table.getRandomColumn();
            sb.append(randomColumn.getName());
            if (Randomly.getBoolean()) {
                sb.append(" SET DEFAULT ").append(generator
                        .generateConstant(randomColumn.getType().getPrimitiveDataType(), Randomly.getBoolean()));
            } else {
                sb.append(" DROP DEFAULT");
            }
            break;
        case CHANGE_COLUMN:
            sb.append(Randomly.fromOptions("CHANGE COLUMN ", "CHANGE "));
            String oldColumnName = table.getRandomColumn().getName();
            String newColumnName = table.getFreeColumnName();
            sb.append(oldColumnName).append(" ").append(newColumnName).append(" ");
            StoneDBDataType.appendTypeAndValue(StoneDBDataType.getRandomWithoutNull(), sb, globalState.getRandomly());
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
            sb.append(Randomly.fromOptions(" TO ", " AS "));
            sb.append(table.getFreeColumnName());
            break;
        default:
            throw new AssertionError(action);
        }
    }
}
