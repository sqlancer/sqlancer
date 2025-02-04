package sqlancer.presto.gen;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.presto.PrestoGlobalState;
import sqlancer.presto.PrestoSchema.PrestoCompositeDataType;
import sqlancer.presto.PrestoSchema.PrestoTable;

public final class PrestoAlterTableGenerator {

    private PrestoAlterTableGenerator() {
    }

    public static SQLQueryAdapter getQuery(PrestoGlobalState globalState) {
        ExpectedErrors errors = new ExpectedErrors();
        StringBuilder sb = new StringBuilder("ALTER TABLE ");
        PrestoTable table = globalState.getSchema().getRandomTable(t -> !t.isView());
        // PrestoTypedExpressionGenerator gen = new
        // PrestoTypedExpressionGenerator(globalState).setColumns(table.getColumns());
        sb.append(table.getName());
        sb.append(" ");
        Action action = Randomly.fromOptions(Action.values());
        switch (action) {
        case ADD_COLUMN:
            sb.append("ADD COLUMN ");
            String columnName = table.getFreeColumnName();
            sb.append(columnName);
            sb.append(" ");
            sb.append(PrestoCompositeDataType.getRandomWithoutNull());
            break;
        case ALTER_COLUMN:
            sb.append("ALTER COLUMN ");
            sb.append(table.getRandomColumn().getName());
            sb.append(" SET DATA TYPE ");
            sb.append(PrestoCompositeDataType.getRandomWithoutNull());
            // if (Randomly.getBoolean()) {
            // sb.append(" USING ");
            // PrestoErrors.addExpressionErrors(errors);
            // sb.append(PrestoToStringVisitor.asString(gen.generateExpression()));
            // }
            errors.add("Cannot change the type of this column: an index depends on it!");
            errors.add("Cannot change the type of a column that has a UNIQUE or PRIMARY KEY constraint specified");
            errors.add("Unimplemented type for cast");
            errors.add("Conversion:");
            errors.add("Cannot change the type of a column that has a CHECK constraint specified");
            break;
        case DROP_COLUMN:
            sb.append("DROP COLUMN ");
            sb.append(table.getRandomColumn().getName());
            errors.add("named in key does not exist"); // TODO
            errors.add("Cannot drop this column:");
            errors.add("Cannot drop column: table only has one column remaining!");
            errors.add("because there is a CHECK constraint that depends on it");
            errors.add("because there is a UNIQUE constraint that depends on it");
            break;
        default:
            throw new AssertionError(action);
        }
        return new SQLQueryAdapter(sb.toString(), errors, true, false);
    }

    enum Action {
        ADD_COLUMN, ALTER_COLUMN, DROP_COLUMN
    }

}
