package sqlancer.oceanbase.gen;

import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.Randomly;
import sqlancer.oceanbase.*;


import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class OceanBaseUpdateGenerator {
    private OceanBaseUpdateGenerator() {
    }

    public static SQLQueryAdapter getQuery(OceanBaseGlobalState globalState) throws SQLException {
        Randomly r = globalState.getRandomly();
        ExpectedErrors errors = new ExpectedErrors();
        OceanBaseSchema.OceanBaseTable table = globalState.getSchema().getRandomTable(t -> !t.isView());
        OceanBaseExpressionGenerator gen = new OceanBaseExpressionGenerator(globalState).setColumns(table.getColumns());
        StringBuilder sb = new StringBuilder("UPDATE ");
        if (Randomly.getBoolean()) {
            sb.append(" /*+parallel("+r.getInteger(0, 10)+") enable_parallel_dml*/ ");
        }
        sb.append(table.getName());
        sb.append(" SET ");
        List<OceanBaseSchema.OceanBaseColumn> columns = table.getRandomNonEmptyColumnSubset();
        for (int i = 0; i < columns.size(); i++) {
            if (i != 0) {
                sb.append(", ");
            }
            sb.append(columns.get(i).getName());
            sb.append("=");
            if (Randomly.getBoolean()) {
                sb.append(gen.generateConstant(columns.get(i)));
            } else {
                sb.append(OceanBaseVisitor.asString(gen.generateExpression()));
                OceanBaseErrors.addExpressionErrors(errors);
            }
        }
        if (Randomly.getBoolean()) {
            sb.append(" WHERE ");
            OceanBaseErrors.addExpressionErrors(errors);
            sb.append(OceanBaseVisitor.asString(gen.generateExpression()));
            errors.add("Data Too Long"); 
        }
        errors.add("Duplicated primary key");
        OceanBaseErrors.addInsertErrors(errors);

        return new SQLQueryAdapter(sb.toString(), errors);
    }
}
