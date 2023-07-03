package sqlancer.stonedb.gen;

import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.gen.AbstractInsertGenerator;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.schema.AbstractTableColumn;
import sqlancer.stonedb.StoneDBProvider.StoneDBGlobalState;
import sqlancer.stonedb.StoneDBSchema.StoneDBColumn;
import sqlancer.stonedb.StoneDBSchema.StoneDBTable;
import sqlancer.stonedb.StoneDBToStringVisitor;

public class StoneDBInsertGenerator extends AbstractInsertGenerator<StoneDBColumn> {
    private final StoneDBGlobalState globalState;
    // which table to insert into
    private final StoneDBTable table;
    // which subset columns of the table to add values
    private final List<StoneDBColumn> columns;
    private final StringBuilder sb = new StringBuilder();
    ExpectedErrors errors = new ExpectedErrors();

    public StoneDBInsertGenerator(StoneDBGlobalState globalState) {
        this.globalState = globalState;
        table = globalState.getSchema().getRandomTable();
        columns = table.getRandomNonEmptyColumnSubset();
    }

    public static SQLQueryAdapter generate(StoneDBGlobalState globalState) {
        return new StoneDBInsertGenerator(globalState).getQuery();
    }

    private SQLQueryAdapter getQuery() {
        sb.append("INSERT");
        if (Randomly.getBoolean()) {
            sb.append(" ");
            sb.append(Randomly.fromOptions("LOW_PRIORITY", "DELAYED", "HIGH_PRIORITY"));
        }
        if (Randomly.getBoolean()) {
            sb.append(" IGNORE");
        }
        sb.append(" INTO ");
        sb.append(table.getName());
        appendPartition();
        appendColumnsAndValues(columns);
        addExpectedErrors();
        return new SQLQueryAdapter(sb.toString(), errors);
    }

    private void addExpectedErrors() {
        // com.mysql.cj.jdbc.exceptions.MysqlDataTruncation: Data truncation: Out of range value for column 'c0' at row 1
        errors.add("Data truncation: Out of range value for column ");
        // java.sql.SQLSyntaxErrorException: Unknown column 'c0' in 'field list'
        errors.add("Unknown column ");
        // Caused by: acom.mysql.cj.jdbc.exceptions.MysqlDataTruncation: Data truncation: Incorrect datetime value: '1969-12-08 01:07:14' for column 'c0' at row 3
        // Reason: The TIMESTAMP data type is used for values that contain both date and time parts. TIMESTAMP has a range of '1970-01-01 00:00:01' UTC to '2038-01-19 03:14:07' UTC.
        // Reference: https://dev.mysql.com/doc/refman/5.7/en/datetime.html#:~:text=The%20TIMESTAMP%20data%20type%20is%20used%20for%20values%20that%20contain%20both%20date%20and%20time%20parts.%20TIMESTAMP%20has%20a%20range%20of%20%271970%2D01%2D01%2000%3A00%3A01%27%20UTC%20to%20%272038%2D01%2D19%2003%3A14%3A07%27%20UTC.
        errors.add("Data truncation: Incorrect datetime value: ");
    }

    private void appendPartition() {

    }

    private void appendColumnsAndValues(List<StoneDBColumn> columns) {
        sb.append("(");
        sb.append(columns.stream().map(AbstractTableColumn::getName).collect(Collectors.joining(", ")));
        sb.append(")");
        sb.append(Randomly.fromOptions(" VALUES ", " VALUE "));
        appendValues();
    }

    // append nrRows rows
    private void appendValues() {
        int nrRows;
        if (Randomly.getBoolean()) {
            nrRows = 1;
        } else {
            nrRows = 1 + Randomly.smallNumber();
        }
        for (int row = 0; row < nrRows; row++) {
            appendOneValue(row);
        }
    }

    // append all columns of one row
    private void appendOneValue(int nrRow) {
        if (nrRow != 0) {
            sb.append(", ");
        }
        sb.append("(");
        for (int c = 0; c < columns.size(); c++) {
            if (c != 0) {
                sb.append(", ");
            }
            insertValue(columns.get(c));
        }
        sb.append(")");
    }

    // append one column of one row
    @Override
    protected void insertValue(StoneDBColumn column) {
        if (Randomly.getBooleanWithRatherLowProbability()) {
            sb.append("DEFAULT");
        } else {
            sb.append(StoneDBToStringVisitor.asString(new StoneDBExpressionGenerator(globalState)
                    .generateConstant(column.getType().getPrimitiveDataType(), column.isNullable())));
        }
    }
}
