package sqlancer.yugabyte.ycql.gen;

import java.util.List;
import java.util.stream.Collectors;

import sqlancer.common.gen.AbstractInsertGenerator;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.schema.AbstractTableColumn;
import sqlancer.yugabyte.ycql.YCQLErrors;
import sqlancer.yugabyte.ycql.YCQLProvider.YCQLGlobalState;
import sqlancer.yugabyte.ycql.YCQLSchema.YCQLColumn;
import sqlancer.yugabyte.ycql.YCQLSchema.YCQLTable;
import sqlancer.yugabyte.ycql.YCQLToStringVisitor;

public class YCQLInsertGenerator extends AbstractInsertGenerator<YCQLColumn> {

    private final YCQLGlobalState globalState;
    private final ExpectedErrors errors = new ExpectedErrors();

    public YCQLInsertGenerator(YCQLGlobalState globalState) {
        this.globalState = globalState;
    }

    public static SQLQueryAdapter getQuery(YCQLGlobalState globalState) {
        return new YCQLInsertGenerator(globalState).generate();
    }

    private SQLQueryAdapter generate() {
        sb.append("INSERT INTO ");
        YCQLTable table = globalState.getSchema().getRandomTable(t -> !t.isView());
        List<YCQLColumn> columns = table.getColumns();
        sb.append(globalState.getDatabaseName()).append(".").append(table.getName());
        sb.append("(");
        sb.append(columns.stream().map(AbstractTableColumn::getName).collect(Collectors.joining(", ")));
        sb.append(")");
        sb.append(" VALUES ");
        insertColumns(columns);

        errors.add("Invalid Arguments");
        errors.add("Null Argument for Primary Key");

        YCQLErrors.addExpressionErrors(errors);
        return new SQLQueryAdapter(sb.toString(), errors);
    }

    @Override
    protected void insertColumns(List<YCQLColumn> columns) {
        sb.append("(");
        for (int nrColumn = 0; nrColumn < columns.size(); nrColumn++) {
            if (nrColumn != 0) {
                sb.append(", ");
            }
            insertValue(columns.get(nrColumn));
        }
        sb.append(")");
    }

    @Override
protected void insertValue(YCQLColumn columnYCQL) {
    // Generate a meaningful value based on - column's data type
    switch (columnYCQL.getType()) {
        case INT:
            // Generate a random integer within - reasonable range
            sb.append(globalState.getRandomly().getInteger(-1000, 1000));
            break;
        case TEXT:
        case VARCHAR:
            // Generate a random string of length - 10
            sb.append("'").append(globalState.getRandomly().getString(10)).append("'");
            break;
        case BOOLEAN:
            // Generate a random boolean value
            sb.append(globalState.getRandomly().getBoolean());
            break;
        case FLOAT:
        case DOUBLE:
            // Generate a random floating-point number
            sb.append(globalState.getRandomly().getDouble());
            break;
        case UUID:
            // Generate a random UUID
            sb.append("uuid()");
            break;
        case TIMESTAMP:
            // Generate a random timestamp
            sb.append("'").append(globalState.getRandomly().getTimestamp()).append("'");
            break;
        case BLOB:
            // Generate a random binary value (e.g., a hex string)
            sb.append("0x").append(globalState.getRandomly().getHexString(10));
            break;
        default:
            // Fallback to a random constant if the type is unknown
            sb.append(YCQLToStringVisitor.asString(new YCQLExpressionGenerator(globalState).generateConstant()));
            break;
    }
}

}
