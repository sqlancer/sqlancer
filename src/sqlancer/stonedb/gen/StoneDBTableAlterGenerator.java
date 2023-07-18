package sqlancer.stonedb.gen;

import java.util.regex.Pattern;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.stonedb.StoneDBProvider.StoneDBGlobalState;
import sqlancer.stonedb.StoneDBSchema.StoneDBColumn;
import sqlancer.stonedb.StoneDBSchema.StoneDBDataType;
import sqlancer.stonedb.StoneDBSchema.StoneDBTable;

public class StoneDBTableAlterGenerator {
    private final StoneDBGlobalState globalState;
    private final StringBuilder sb = new StringBuilder();
    private final StoneDBTable table;
    ExpectedErrors errors = new ExpectedErrors();

    enum Action {
        ADD_COLUMN, ALTER_COLUMN, DROP_COLUMN, CHANGE_COLUMN
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
        appendAlterOption(Randomly.fromOptions(Action.values()));
        addExpectedErrors();
        return new SQLQueryAdapter(sb.toString(), errors, true);
    }

    private void addExpectedErrors() {
        // com.mysql.cj.jdbc.exceptions.MysqlDataTruncation: Data truncation: Data too long for column 'c0' at row 2
        errors.addRegex(Pattern.compile("Data truncation: Data too long for column 'c\\d{1,3}' at row \\d{1,3}"));
        // com.mysql.cj.jdbc.exceptions.MysqlDataTruncation: Data truncation: Incorrect datetime value:
        // '0.571272522740968' for column 'c1' at row 1
        errors.add("Incorrect datetime value: ");
        // java.sql.SQLSyntaxErrorException: Invalid default value for 'c0'
        errors.add("Invalid default value for ");
        // com.mysql.cj.jdbc.exceptions.MysqlDataTruncation: Data truncation: Out of range value for column 'c0' at row
        // 2
        errors.add("Data truncation: Out of range value for column ");
        // java.sql.SQLSyntaxErrorException: Specified key was too long; max key length is 3072 bytes
        errors.add("Specified key was too long; max key length is 3072 bytes");
        // java.sql.SQLSyntaxErrorException: You can't delete all columns with ALTER TABLE; use DROP TABLE instead
        errors.add("You can't delete all columns with ALTER TABLE; use DROP TABLE instead");
        // java.sql.SQLSyntaxErrorException: Unknown column 'c0' in 't1'
        errors.addRegex(Pattern.compile("Unknown column 'c\\d{1,3}' in 't\\d{1,3}'"));
        // java.sql.SQLSyntaxErrorException: BLOB, TEXT, GEOMETRY or JSON column 'c0' can't have a default value
        errors.addRegex(Pattern.compile("BLOB, TEXT, GEOMETRY or JSON column 'c\\d{1,3}' can't have a default value"));
        // java.sql.SQLSyntaxErrorException: Column length too big for column 'c91' (max = 16383); use BLOB or TEXT
        // instead
        errors.addRegex(Pattern
                .compile("Column length too big for column 'c\\d{1,3}' \\(max = 16383\\); use BLOB or TEXT instead"));
    }

    private void appendAlterOption(Action action) {
        StoneDBExpressionGenerator generator = new StoneDBExpressionGenerator(globalState)
                .setColumns(table.getColumns());
        switch (action) {
        case ADD_COLUMN:
            sb.append("ADD COLUMN ");
            String columnName = table.getFreeColumnName();
            sb.append(" ").append(columnName).append(" ");
            sb.append(StoneDBDataType.getTypeAndValue(StoneDBDataType.getRandomWithoutNull()));
            // java.sql.SQLSyntaxErrorException: Column length too big for column 'c1' (max = 16383); use BLOB or TEXT
            // instead
            errors.addRegex(Pattern
                    .compile("Column length too big for column 'c\\d{1,3}' (max = 16383); use BLOB or TEXT instead"));
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
            sb.append(StoneDBDataType.getTypeAndValue(StoneDBDataType.getRandomWithoutNull()));
            // java.sql.SQLSyntaxErrorException: Column length too big for column 'c1' (max = 16383); use BLOB or TEXT
            // instead
            errors.addRegex(Pattern
                    .compile("Column length too big for column 'c\\d{1,3}' (max = 16383); use BLOB or TEXT instead"));
            // java.sql.SQLSyntaxErrorException: BLOB column 'c1' can't be used in key specification with the used table
            // type
            errors.addRegex(Pattern
                    .compile("BLOB column 'c\\d{1,3}' can't be used in key specification with the used table type"));
            if (Randomly.getBoolean()) {
                if (Randomly.getBoolean()) {
                    sb.append(" FIRST");
                } else {
                    sb.append(" AFTER ");
                    sb.append(table.getRandomColumn().getName());
                }
            }
            break;
        default:
            throw new AssertionError(action);
        }
    }
}
