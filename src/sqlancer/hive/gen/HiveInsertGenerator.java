package sqlancer.hive.gen;

import java.util.List;

import sqlancer.common.gen.AbstractInsertGenerator;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.hive.HiveErrors;
import sqlancer.hive.HiveGlobalState;
import sqlancer.hive.HiveSchema.HiveColumn;
import sqlancer.hive.HiveSchema.HiveTable;
import sqlancer.hive.HiveToStringVisitor;

public class HiveInsertGenerator extends AbstractInsertGenerator<HiveColumn> {

    private final HiveGlobalState globalState;
    private final ExpectedErrors errors = new ExpectedErrors();
    private final HiveExpressionGenerator gen;

    public HiveInsertGenerator(HiveGlobalState globalState) {
        this.globalState = globalState;
        this.gen = new HiveExpressionGenerator(globalState);
    }

    public static SQLQueryAdapter getQuery(HiveGlobalState globalState) {
        return new HiveInsertGenerator(globalState).generate();
    }

    @Override
    protected void insertValue(HiveColumn column) {
        sb.append(HiveToStringVisitor.asString(gen.generateConstant()));
    }

    private SQLQueryAdapter generate() {
        // Inserting values into tables from SQL.
        sb.append("INSERT INTO ");
        HiveTable table = globalState.getSchema().getRandomTable(t -> !t.isView());
        sb.append(table.getName());

        // TODO: specify the inserted partition

        sb.append(" VALUES ");

        // Values must be provided by every column in the Hive table.
        // A value is either null or any valid SQL literal.
        List<HiveColumn> columns = table.getColumns();
        insertColumns(columns);

        HiveErrors.addInsertErrors(errors);
        return new SQLQueryAdapter(sb.toString(), errors, false, false);
    }
}
