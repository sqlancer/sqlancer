package sqlancer.questdb.gen;

import sqlancer.common.gen.AbstractInsertGenerator;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.questdb.QuestDBProvider.QuestDBGlobalState;
import sqlancer.questdb.QuestDBSchema;
import sqlancer.questdb.QuestDBSchema.QuestDBColumn;

public class QuestDBInsertGenerator extends AbstractInsertGenerator<QuestDBColumn> {

    private final QuestDBGlobalState globalState;

    // uncomment later:
    // private final ExpectedErrors errors = new ExpectedErrors();

    public QuestDBInsertGenerator(QuestDBGlobalState globalState) {
        this.globalState = globalState;
    }

    private SQLQueryAdapter generate() {
        // TODO: below is dummy implementation to pass compilation
        QuestDBSchema.QuestDBTable table = globalState.getSchema().getRandomTable();
        table.getRandomNonEmptyColumnSubset();
        return null;
    }

    public static SQLQueryAdapter getQuery(QuestDBGlobalState globalState) {
        return new QuestDBInsertGenerator(globalState).generate();
    }

    @Override
    protected void insertValue(QuestDBColumn tiDBColumn) {
        // TODO
    }
}
