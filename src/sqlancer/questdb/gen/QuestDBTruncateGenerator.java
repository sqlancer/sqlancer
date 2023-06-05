package sqlancer.questdb.gen;

import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.questdb.QuestDBErrors;
import sqlancer.questdb.QuestDBProvider.QuestDBGlobalState;
import sqlancer.questdb.QuestDBSchema.QuestDBTable;

public final class QuestDBTruncateGenerator {

    public static SQLQueryAdapter getQuery(QuestDBGlobalState globalState) {
        StringBuilder sb = new StringBuilder("TRUNCATE TABLE ");
        ExpectedErrors errors = new ExpectedErrors();
        QuestDBTable table = globalState.getSchema().getRandomTable(t -> !t.isView());
        sb.append('\'').append(table.getName()).append('\'');
        QuestDBErrors.addExpressionErrors(errors);
        return new SQLQueryAdapter(sb.toString(), errors);
    }
}
