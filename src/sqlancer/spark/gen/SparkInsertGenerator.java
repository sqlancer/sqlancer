package sqlancer.spark.gen;

import java.util.List;

import sqlancer.common.gen.AbstractInsertGenerator;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.spark.SparkErrors;
import sqlancer.spark.SparkGlobalState;
import sqlancer.spark.SparkSchema.SparkColumn;
import sqlancer.spark.SparkSchema.SparkTable;
import sqlancer.spark.SparkToStringVisitor;

public class SparkInsertGenerator extends AbstractInsertGenerator<SparkColumn> {

    private final SparkGlobalState globalState;
    private final SparkExpressionGenerator gen;

    public SparkInsertGenerator(SparkGlobalState globalState) {
        this.globalState = globalState;
        this.gen = new SparkExpressionGenerator(globalState);
        this.canonicalizeString = false;
    }

    public static SQLQueryAdapter getQuery(SparkGlobalState globalState) {
        return new SparkInsertGenerator(globalState).getStatement();
    }

    @Override
    protected void insertValue(SparkColumn column) {
        sb.append(SparkToStringVisitor.asString(gen.generateConstant()));
    }

    @Override
    public void buildStatement() {
        sb.append("INSERT INTO ");
        SparkTable table = globalState.getSchema().getRandomTable(t -> !t.isView());
        sb.append(table.getName());

        sb.append(" VALUES ");

        List<SparkColumn> columns = table.getColumns();
        insertColumns(columns);

        SparkErrors.addInsertErrors(errors);
    }
}
