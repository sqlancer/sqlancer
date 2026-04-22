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
    private final DataFusionTable targetTable;

    public DataFusionInsertGenerator(DataFusionGlobalState globalState, DataFusionTable targetTable) {
        this.globalState = globalState;
        this.targetTable = targetTable;
    }

    public static SQLQueryAdapter getQuery(DataFusionGlobalState globalState, DataFusionTable targetTable) {
        return new DataFusionInsertGenerator(globalState, targetTable).getStatement();
    }

    @Override
    public void buildStatement() {
        if (targetTable.getColumns().isEmpty()) {
            throw new IgnoreMeException();
        }
        List<DataFusionColumn> columns = targetTable.getRandomNonEmptyColumnSubset();
        buildInsertInto(targetTable.getName(), columns);
    }

    @Override
    protected void insertValue(DataFusionColumn col) {
        String val = DataFusionToStringVisitor
                .asString(new DataFusionExpressionGenerator(globalState).generateConstant(col.getType()));
        sb.append(val);
    }

}
