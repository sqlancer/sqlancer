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
        return new SQLQueryAdapter(sb.toString(), errors);
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

    private void appendOneValue(int nrRow) {
        if (nrRow != 0) {
            sb.append(", ");
        }
        sb.append("(");
        for (int c = 0; c < columns.size(); c++) {
            if (c != 0) {
                sb.append(", ");
            }
            sb.append(StoneDBToStringVisitor.asString(new StoneDBExpressionGenerator(globalState).generateConstant()));

        }
        sb.append(")");
    }

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
