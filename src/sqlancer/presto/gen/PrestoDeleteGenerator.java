package sqlancer.presto.gen;

import sqlancer.Randomly;
import sqlancer.common.gen.AbstractDeleteGenerator;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.presto.PrestoErrors;
import sqlancer.presto.PrestoGlobalState;
import sqlancer.presto.PrestoSchema;
import sqlancer.presto.PrestoSchema.PrestoTable;
import sqlancer.presto.PrestoToStringVisitor;

public final class PrestoDeleteGenerator extends AbstractDeleteGenerator {

    private final PrestoGlobalState globalState;

    private PrestoDeleteGenerator(PrestoGlobalState globalState) {
        this.globalState = globalState;
        this.canonicalizeString = false;
    }

    public static SQLQueryAdapter generate(PrestoGlobalState globalState) {
        return new PrestoDeleteGenerator(globalState).getStatement();
    }

    @Override
    public void buildStatement() {
        sb.append("DELETE FROM ");
        PrestoTable table = globalState.getSchema().getRandomTable(t -> !t.isView());
        sb.append(table.getName());
        if (Randomly.getBoolean()) {
            sb.append(" WHERE ");
            sb.append(PrestoToStringVisitor
                    .asString(new PrestoTypedExpressionGenerator(globalState).setColumns(table.getColumns())
                            .generateExpression(PrestoSchema.PrestoCompositeDataType.getRandomWithoutNull())));
        }
        PrestoErrors.addExpressionErrors(errors);
    }

}
