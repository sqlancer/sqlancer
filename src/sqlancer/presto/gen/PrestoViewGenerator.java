package sqlancer.presto.gen;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.presto.PrestoErrors;
import sqlancer.presto.PrestoGlobalState;
import sqlancer.presto.PrestoToStringVisitor;

public final class PrestoViewGenerator {

    private PrestoViewGenerator() {
    }

    public static SQLQueryAdapter generate(PrestoGlobalState globalState) {
        int nrColumns = Randomly.smallNumber() + 1;
        StringBuilder sb = new StringBuilder("CREATE ");
        sb.append("VIEW ");
        sb.append(globalState.getSchema().getFreeViewName());
        sb.append("(");
        for (int i = 0; i < nrColumns; i++) {
            if (i != 0) {
                sb.append(", ");
            }
            sb.append("c");
            sb.append(i);
        }
        sb.append(") AS ");
        sb.append(PrestoToStringVisitor.asString(PrestoRandomQuerySynthesizer.generateSelect(globalState, nrColumns)));
        ExpectedErrors errors = new ExpectedErrors();
        PrestoErrors.addExpressionErrors(errors);
        PrestoErrors.addGroupByErrors(errors);
        return new SQLQueryAdapter(sb.toString(), errors, true, false);
    }

}
