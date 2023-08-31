package sqlancer.presto.gen;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.presto.PrestoErrors;
import sqlancer.presto.PrestoGlobalState;
import sqlancer.presto.PrestoSchema;
import sqlancer.presto.PrestoSchema.PrestoTable;
import sqlancer.presto.PrestoToStringVisitor;

public final class PrestoDeleteGenerator {

    private PrestoDeleteGenerator() {
    }

    public static SQLQueryAdapter generate(PrestoGlobalState globalState) {
        StringBuilder sb = new StringBuilder("DELETE FROM ");
        ExpectedErrors errors = new ExpectedErrors();
        PrestoTable table = globalState.getSchema().getRandomTable(t -> !t.isView());
        sb.append(table.getName());
        if (Randomly.getBoolean()) {
            sb.append(" WHERE ");
            sb.append(PrestoToStringVisitor
                    .asString(new PrestoTypedExpressionGenerator(globalState).setColumns(table.getColumns())
                            .generateExpression(PrestoSchema.PrestoCompositeDataType.getRandomWithoutNull())));
        }
        PrestoErrors.addExpressionErrors(errors);
        return new SQLQueryAdapter(sb.toString(), errors, false, false);
    }

}
