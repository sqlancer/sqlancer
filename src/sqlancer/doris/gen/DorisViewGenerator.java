package sqlancer.doris.gen;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.doris.DorisErrors;
import sqlancer.doris.DorisProvider.DorisGlobalState;
import sqlancer.doris.visitor.DorisToStringVisitor;

public final class DorisViewGenerator {

    private DorisViewGenerator() {
    }

    public static SQLQueryAdapter getQuery(DorisGlobalState globalState) {
        if (globalState.getSchema().getDatabaseTables().size() > globalState.getDbmsSpecificOptions().maxNumTables) {
            throw new IgnoreMeException();
        }
        int nrColumns = Randomly.smallNumber() + 1;
        StringBuilder sb = new StringBuilder("CREATE VIEW ");
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
        sb.append(DorisToStringVisitor.asString(DorisRandomQuerySynthesizer.generateSelect(globalState, nrColumns)));
        ExpectedErrors errors = new ExpectedErrors();
        DorisErrors.addExpressionErrors(errors);
        return new SQLQueryAdapter(sb.toString(), errors, true);
    }

}
