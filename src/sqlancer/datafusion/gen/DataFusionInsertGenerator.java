package sqlancer.datafusion.gen;

import java.util.List;

import sqlancer.IgnoreMeException;
import sqlancer.common.gen.AbstractInsertGenerator;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.datafusion.DataFusionProvider.DataFusionGlobalState;
import sqlancer.datafusion.DataFusionSchema.DataFusionColumn;
import sqlancer.datafusion.DataFusionSchema.DataFusionTable;
import sqlancer.datafusion.DataFusionToStringVisitor;

public class DataFusionInsertGenerator extends AbstractInsertGenerator<DataFusionColumn> {

    private final DataFusionGlobalState globalState;

    public DataFusionInsertGenerator(DataFusionGlobalState globalState) {
        this.globalState = globalState;
    }

    public static SQLQueryAdapter getQuery(DataFusionGlobalState globalState, DataFusionTable targetTable) {
        return new DataFusionInsertGenerator(globalState).generate(targetTable);
    }

    private SQLQueryAdapter generate(DataFusionTable targetTable) {
        if (targetTable.getColumns().isEmpty()) {
            throw new IgnoreMeException();
        }
        List<DataFusionColumn> columns = targetTable.getRandomNonEmptyColumnSubset();
        buildInsertInto(targetTable.getName(), columns);
        return new SQLQueryAdapter(sb.toString(), errors);
    }

    @Override
    protected void insertValue(DataFusionColumn col) {
        String val = DataFusionToStringVisitor
                .asString(new DataFusionExpressionGenerator(globalState).generateConstant(col.getType()));
        sb.append(val);
    }

}
