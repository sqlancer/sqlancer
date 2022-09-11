package sqlancer.databend.gen;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.databend.DatabendErrors;
import sqlancer.databend.DatabendProvider.DatabendGlobalState;
import sqlancer.databend.DatabendToStringVisitor;

public final class DatabendViewGenerator {

    private DatabendViewGenerator() {
    }

    public static SQLQueryAdapter generate(DatabendGlobalState globalState) {
        int nrColumns = Randomly.smallNumber() + 1;
        StringBuilder sb = new StringBuilder("CREATE ");
        sb.append("VIEW ");
        sb.append(globalState.getSchema().getFreeViewName());
        // sb.append("(");
        // for (int i = 0; i < nrColumns; i++) {
        // if (i != 0) {
        // sb.append(", ");
        // }
        // sb.append("c");
        // sb.append(i);
        // }
        // sb.append(") AS ");
        sb.append(" AS ");
        sb.append(DatabendToStringVisitor
                .asString(DatabendRandomQuerySynthesizer.generateSelect(globalState, nrColumns)));
        ExpectedErrors errors = new ExpectedErrors();
        DatabendErrors.addExpressionErrors(errors);
        DatabendErrors.addGroupByErrors(errors);
        return new SQLQueryAdapter(sb.toString(), errors, true);
    }

}
