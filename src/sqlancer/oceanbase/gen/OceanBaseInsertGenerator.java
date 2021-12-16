package sqlancer.oceanbase.gen;

import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.oceanbase.OceanBaseGlobalState;
import sqlancer.oceanbase.OceanBaseSchema;
import sqlancer.oceanbase.OceanBaseSchema.OceanBaseColumn;
import sqlancer.oceanbase.OceanBaseSchema.OceanBaseTable;
import sqlancer.oceanbase.OceanBaseVisitor;

public class OceanBaseInsertGenerator {

    private final OceanBaseTable table;
    private final StringBuilder sb = new StringBuilder();
    private final ExpectedErrors errors = new ExpectedErrors();
    private final OceanBaseGlobalState globalState;
    private final Randomly r;
    private int type;

    public OceanBaseInsertGenerator(OceanBaseGlobalState globalState) {
        this.globalState = globalState;
        table = globalState.getSchema().getRandomTable();
        this.r = globalState.getRandomly();
    }

    public static SQLQueryAdapter insertRow(OceanBaseGlobalState globalState) throws SQLException {
        if (Randomly.getBoolean()) {
            return new OceanBaseInsertGenerator(globalState).generateInsert();
        } else {
            return new OceanBaseInsertGenerator(globalState).generateReplace();
        }
    }

    private SQLQueryAdapter generateReplace() {
        sb.append("REPLACE");
        type = 1;
        return generateInto();

    }

    private SQLQueryAdapter generateInsert() {
        sb.append("INSERT");
        if (Randomly.getBoolean()) {
            sb.append(" /*+parallel(" + r.getLong(0, 10) + ") enable_parallel_dml*/ ");
        }
        if (Randomly.getBoolean()) {
            sb.append(" ");
        }
        return generateInto();
    }

    private SQLQueryAdapter generateInto() {
        sb.append(" INTO ");
        sb.append(table.getName());
        List<OceanBaseColumn> columns = table.getRandomNonEmptyColumnSubset();
        sb.append("(");
        sb.append(columns.stream().map(c -> c.getName()).collect(Collectors.joining(", ")));
        sb.append(") ");
        sb.append("VALUES");
        OceanBaseExpressionGenerator gen = new OceanBaseExpressionGenerator(globalState).setColumns(table.getColumns());
        int nrRows;
        if (Randomly.getBoolean()) {
            nrRows = 1;
        } else {
            nrRows = 1 + Randomly.smallNumber();
        }
        for (int row = 0; row < nrRows; row++) {
            if (row != 0) {
                sb.append(", ");
            }
            sb.append("(");
            for (int c = 0; c < columns.size(); c++) {
                if (c != 0) {
                    sb.append(", ");
                }
                OceanBaseColumn col = columns.get(c);
                sb.append(OceanBaseVisitor.asString(gen.generateConstant(col)));

            }
            sb.append(")");
        }
        if (Randomly.getBoolean() && type == 0) {
            List<OceanBaseSchema.OceanBaseColumn> upcolumns = table.getRandomNonEmptyColumnSubset();
            if (!upcolumns.isEmpty()) {
                sb.append(" ON DUPLICATE KEY UPDATE ");
                sb.append(upcolumns.get(0).getName());
                sb.append("=");
                sb.append(gen.generateConstant(upcolumns.get(0)));
            }
        }
        errors.add("doesn't have a default value");
        errors.add("Data truncation");
        errors.add("Incorrect integer value");
        errors.add("Duplicate entry");
        errors.add("Data truncated for functional index");
        errors.add("Data truncated for column");
        errors.add("cannot be null");
        errors.add("Incorrect decimal value");
        errors.add("Duplicated primary key");
        return new SQLQueryAdapter(sb.toString(), errors);
    }
}
