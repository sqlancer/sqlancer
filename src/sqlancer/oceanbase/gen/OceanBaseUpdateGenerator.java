package sqlancer.oceanbase.gen;

import java.util.List;

import sqlancer.Randomly;
import sqlancer.common.gen.AbstractUpdateGenerator;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.oceanbase.OceanBaseErrors;
import sqlancer.oceanbase.OceanBaseGlobalState;
import sqlancer.oceanbase.OceanBaseSchema;
import sqlancer.oceanbase.OceanBaseSchema.OceanBaseColumn;
import sqlancer.oceanbase.OceanBaseVisitor;

public class OceanBaseUpdateGenerator extends AbstractUpdateGenerator<OceanBaseColumn> {

    private final OceanBaseGlobalState globalState;
    private OceanBaseExpressionGenerator gen;
    private final Randomly r;

    public OceanBaseUpdateGenerator(OceanBaseGlobalState globalState) {
        this.globalState = globalState;
        this.r = globalState.getRandomly();
    }

    public static SQLQueryAdapter update(OceanBaseGlobalState globalState) {
        return new OceanBaseUpdateGenerator(globalState).generate();
    }

    private SQLQueryAdapter generate() {
        OceanBaseSchema.OceanBaseTable table = globalState.getSchema().getRandomTable(t -> !t.isView());
        List<OceanBaseSchema.OceanBaseColumn> columns = table.getRandomNonEmptyColumnSubset();
        gen = new OceanBaseExpressionGenerator(globalState).setColumns(table.getColumns());
        sb.append("UPDATE ");
        if (Randomly.getBoolean()) {
            sb.append(" /*+parallel(" + r.getInteger(0, 10) + ") enable_parallel_dml*/ ");
        }
        sb.append(table.getName());
        sb.append(" SET ");
        updateColumns(columns);
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

    @Override
    protected void updateValue(OceanBaseColumn column) {
        if (Randomly.getBoolean()) {
            sb.append(gen.generateConstant(column));
        } else {
            sb.append(OceanBaseVisitor.asString(gen.generateExpression()));
            OceanBaseErrors.addExpressionErrors(errors);
        }
    }
}
