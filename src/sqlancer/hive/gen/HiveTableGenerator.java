package sqlancer.hive.gen;

import sqlancer.Randomly;
import sqlancer.common.DBMSCommon;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.hive.HiveErrors;
import sqlancer.hive.HiveGlobalState;
import sqlancer.hive.HiveSchema;
import sqlancer.hive.HiveToStringVisitor;
import sqlancer.hive.HiveSchema.HiveColumn;
import sqlancer.hive.HiveSchema.HiveDataType;
import sqlancer.hive.HiveSchema.HiveTable;

import java.util.ArrayList;
import java.util.List;

public class HiveTableGenerator {

    // TODO: support various file formats? e.g. JSONFILE, SEQUENCEFILE, TEXTFILE, RCFILE, ORC, PARQUET, AVRO.

    private enum ColumnConstraints {
        PRIMARY_KEY_DISABLE, UNIQUE_DISABLE, NOT_NULL, DEFAULT, CHECK
        // ENABLE_OR_DISABLE, NOVALIDATE, RELY_OR_NORELY
    }

    private final HiveGlobalState globalState;
    private final String tableName;
    private final boolean allowPrimaryKey = Randomly.getBoolean();
    private final StringBuilder sb = new StringBuilder();
    private final HiveExpressionGenerator gen;
    private final HiveTable table;
    private final List<HiveColumn> columnsToBeAdded = new ArrayList<>();
    private boolean setPrimaryKey = false;

    public HiveTableGenerator(HiveGlobalState globalState, String tableName) {
        this.tableName = tableName;
        this.globalState = globalState;
        this.table = new HiveTable(tableName, columnsToBeAdded, false);
        this.gen = new HiveExpressionGenerator(globalState).setColumns(columnsToBeAdded);
    }

    public static SQLQueryAdapter generate(HiveGlobalState globalState, String tableName) {
        HiveTableGenerator generator = new HiveTableGenerator(globalState, tableName);
        return generator.create();
    }

    private SQLQueryAdapter create() {
        ExpectedErrors errors = new ExpectedErrors();

        sb.append("CREATE TABLE ");
        sb.append(globalState.getDatabaseName());
        sb.append(".");
        sb.append(tableName);
        sb.append(" (");
        for (int i = 0; i < Randomly.smallNumber() + 1; i++) {
            if (i != 0) {
                sb.append(", ");
            }
            appendColumn(i, errors);
        }
        sb.append(")");

        // TODO: implement PARTITION BY clause
        // TODO: implement CLUSTERED BY, SKEWED BY clauses
        // TODO: implement ROW FORMAT and STORED AS clauses
        // TODO: randomly add some predefined TABLEPROPERTIES
        // TODO: implement CTAS (AS clause)

        HiveErrors.addExpressionErrors(errors);
        return new SQLQueryAdapter(sb.toString(), errors, true, false);
    }

    private void appendColumn(int columnId, ExpectedErrors errors) {
        String columnName = DBMSCommon.createColumnName(columnId);
        sb.append(columnName);
        sb.append(" ");
        HiveDataType randType = HiveSchema.HiveDataType.getRandomType();
        sb.append(randType);
        columnsToBeAdded.add(new HiveColumn(columnName, table, randType));
        appendColumnConstraint(randType, errors);
    }

    private void appendColumnConstraint(HiveDataType type, ExpectedErrors errors) {
        /*
         * column_constraint_specification: : [ PRIMARY KEY|UNIQUE|NOT NULL|DEFAULT [default_value]|CHECK
         * [check_expression] ENABLE|DISABLE NOVALIDATE RELY/NORELY ]
         */
        if (Randomly.getBoolean()) {
            // no column constraint
            return;
        }

        ColumnConstraints constraint = Randomly.fromOptions(ColumnConstraints.values());
        switch (constraint) {
        case PRIMARY_KEY_DISABLE:
            if (allowPrimaryKey && !setPrimaryKey) {
                sb.append(" PRIMARY KEY DISABLE");
                setPrimaryKey = true;
            }
            break;
        case UNIQUE_DISABLE:
            sb.append(" UNIQUE DISABLE");
            break;
        case NOT_NULL:
            sb.append(" NOT NULL");
            break;
        case DEFAULT:
            sb.append(" DEFAULT (");
            sb.append(HiveToStringVisitor.asString(gen.generateConstant()));
            sb.append(")");
        case CHECK:
            sb.append(" CHECK (");
            sb.append(HiveToStringVisitor.asString(gen.generateExpression()));
            sb.append(")");
            break;
        default:
            throw new AssertionError(constraint);
        }
    }

}
