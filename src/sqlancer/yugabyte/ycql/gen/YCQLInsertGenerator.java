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
    protected void insertValue(YCQLColumn tiDBColumn) {
        // TODO: select a more meaningful value
        sb.append(YCQLToStringVisitor.asString(new YCQLExpressionGenerator(globalState).generateConstant()));
    }

}
