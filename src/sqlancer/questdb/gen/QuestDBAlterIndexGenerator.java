package sqlancer.questdb.gen;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.questdb.QuestDBProvider.QuestDBGlobalState;
import sqlancer.questdb.QuestDBSchema.QuestDBColumn;
import sqlancer.questdb.QuestDBDataType;
import sqlancer.questdb.QuestDBSchema.QuestDBTable;

public final class QuestDBAlterIndexGenerator {

    public static SQLQueryAdapter getQuery(QuestDBGlobalState globalState) {
        ExpectedErrors errors = new ExpectedErrors();
        errors.add(" does not have a column with name \"rowid\"");
        errors.add("Table does not contain column rowid referenced in alter statement");
        errors.add("cannot create index");
        errors.add("Index flag is only supported for SYMBOL");
        errors.add("Invalid column: ");

        StringBuilder sb = new StringBuilder("ALTER TABLE ");

        QuestDBTable table = globalState.getSchema().getRandomTable(t -> true);
        sb.append('\'').append(table.getName()).append('\'');
        sb.append(" ALTER COLUMN ");

        // We should always choose column with SYMBOL type
        QuestDBColumn columnWithSymbolType = table
                .getRandomColumnOrBailout(c -> c.getType() == QuestDBDataType.SYMBOL);

        String columnName = columnWithSymbolType.getName();

        sb.append(columnName);
        sb.append(' ');

        Action action = Randomly.fromOptions(Action.values());
        switch (action) {
            case ADD_INDEX:
                sb.append("ADD INDEX");
                errors.add("already exists!");

                break;
            case DROP_INDEX:
                sb.append("DROP INDEX");
                errors.add("Column is not indexed");
                break;
            default:
                throw new AssertionError("unkown action:" + action);
        }

        return new SQLQueryAdapter(sb.toString(), errors, true);
    }

    enum Action {
        ADD_INDEX, DROP_INDEX
    }
}
