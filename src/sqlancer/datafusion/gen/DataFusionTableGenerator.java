package sqlancer.datafusion.gen;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.datafusion.DataFusionProvider.DataFusionGlobalState;
import sqlancer.datafusion.DataFusionSchema.DataFusionDataType;

public class DataFusionTableGenerator {

    // Randomly generate a query like 'create table t1 (v1 bigint, v2 boolean)'
    public SQLQueryAdapter getQuery(DataFusionGlobalState globalState) {
        ExpectedErrors errors = new ExpectedErrors();
        StringBuilder sb = new StringBuilder();
        String tableName = globalState.getSchema().getFreeTableName();
        sb.append("CREATE TABLE ");
        sb.append(tableName);
        sb.append("(");

        int colCount = Randomly.smallNumber() + 1 + (Randomly.getBoolean() ? 1 : 0);
        for (int i = 0; i < colCount; i++) {
            sb.append("v").append(i).append(" ").append(DataFusionDataType.getRandomWithoutNull().toString());

            if (i != colCount - 1) {
                sb.append(", ");
            }
        }

        sb.append(");");

        return new SQLQueryAdapter(sb.toString(), errors, true);
    }
}
