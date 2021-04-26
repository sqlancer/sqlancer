package sqlancer.tidb.gen;

import java.util.stream.Collectors;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.tidb.TiDBBugs;
import sqlancer.tidb.TiDBProvider.TiDBGlobalState;
import sqlancer.tidb.TiDBSchema.TiDBColumn;
import sqlancer.tidb.TiDBSchema.TiDBDataType;
import sqlancer.tidb.TiDBSchema.TiDBTable;

public final class TiDBAlterTableGenerator {

    private TiDBAlterTableGenerator() {
    }

    private enum Action {
        MODIFY_COLUMN, ENABLE_DISABLE_KEYS, FORCE, DROP_PRIMARY_KEY, ADD_PRIMARY_KEY, CHANGE, DROP_COLUMN, ORDER_BY
    }

    public static SQLQueryAdapter getQuery(TiDBGlobalState globalState) {
        ExpectedErrors errors = new ExpectedErrors();
        errors.add(
                "Information schema is changed during the execution of the statement(for example, table definition may be updated by other DDL ran in parallel)");
        StringBuilder sb = new StringBuilder("ALTER TABLE ");
        TiDBTable table = globalState.getSchema().getRandomTable(t -> !t.isView());
        TiDBColumn column = table.getRandomColumn();
        sb.append(table.getName());
        Action a = Randomly.fromOptions(Action.values());
        sb.append(" ");
        switch (a) {
        case MODIFY_COLUMN:
            if (TiDBBugs.bug10) {
                throw new IgnoreMeException();
            }
            sb.append("MODIFY ");
            sb.append(column.getName());
            sb.append(" ");
            sb.append(TiDBDataType.getRandom());
            errors.add("Unsupported modify column");
            break;
        case DROP_COLUMN:
            sb.append(" DROP ");
            if (table.getColumns().size() <= 1) {
                throw new IgnoreMeException();
            }
            sb.append(column.getName());
            errors.add("with composite index covered or Primary Key covered now");
            errors.add("Unsupported drop integer primary key");
            errors.add("has a generated column dependency");
            errors.add(
                    "references invalid table(s) or column(s) or function(s) or definer/invoker of view lack rights to use them");
            break;
        case ENABLE_DISABLE_KEYS:
            sb.append(Randomly.fromOptions("ENABLE", "DISABLE"));
            sb.append(" KEYS");
            break;
        case FORCE:
            sb.append("FORCE");
            break;
        case DROP_PRIMARY_KEY:
            if (!column.isPrimaryKey()) {
                throw new IgnoreMeException();
            }
            errors.add("Unsupported drop integer primary key");
            errors.add("Unsupported drop primary key when alter-primary-key is false");
            errors.add("Unsupported drop primary key when the table's pkIsHandle is true");
            errors.add("Incorrect table definition; there can be only one auto column and it must be defined as a key");
            sb.append(" DROP PRIMARY KEY");
            break;
        case ADD_PRIMARY_KEY:
            sb.append("ADD PRIMARY KEY(");
            sb.append(table.getRandomNonEmptyColumnSubset().stream().map(c -> {
                StringBuilder colName = new StringBuilder(c.getName());
                if (c.getType().getPrimitiveDataType() == TiDBDataType.TEXT
                        || c.getType().getPrimitiveDataType() == TiDBDataType.BLOB) {
                    TiDBTableGenerator.appendSpecifiers(colName, c.getType().getPrimitiveDataType());
                }
                return colName;
            }).collect(Collectors.joining(", ")));
            sb.append(")");
            errors.add("Unsupported add primary key, alter-primary-key is false");
            errors.add("Information schema is changed during the execution of the statement");
            errors.add("Multiple primary key defined");
            errors.add("Invalid use of NULL value");
            errors.add("Duplicate entry");
            errors.add("'Defining a virtual generated column as primary key' is not supported for generated columns");
            break;
        case CHANGE:
            if (TiDBBugs.bug10) {
                throw new IgnoreMeException();
            }
            sb.append(" CHANGE ");
            sb.append(column.getName());
            sb.append(" ");
            sb.append(column.getName());
            sb.append(" ");
            sb.append(column.getType().getPrimitiveDataType());
            sb.append(" NOT NULL ");
            errors.add("Invalid use of NULL value");
            errors.add("Unsupported modify column:");
            errors.add("Invalid integer format for value");
            break;
        case ORDER_BY:
            sb.append(" ORDER BY ");
            sb.append(table.getRandomNonEmptyColumnSubset().stream()
                    .map(c -> c.getName() + Randomly.fromOptions("", " ASC", " DESC"))
                    .collect(Collectors.joining(", ")));
            break;
        default:
            throw new AssertionError(a);
        }
        return new SQLQueryAdapter(sb.toString(), errors, true);
    }

}
