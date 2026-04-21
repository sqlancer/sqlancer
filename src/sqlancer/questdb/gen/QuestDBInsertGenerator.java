package sqlancer.questdb.gen;

import java.util.List;

import sqlancer.common.gen.AbstractInsertGenerator;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.questdb.QuestDBErrors;
import sqlancer.questdb.QuestDBProvider.QuestDBGlobalState;
import sqlancer.questdb.QuestDBSchema.QuestDBColumn;
import sqlancer.questdb.QuestDBSchema.QuestDBTable;
import sqlancer.questdb.QuestDBToStringVisitor;

public class QuestDBInsertGenerator extends AbstractInsertGenerator<QuestDBColumn> {

    private final QuestDBGlobalState globalState;

    public QuestDBInsertGenerator(QuestDBGlobalState globalState) {
        this.globalState = globalState;
    }

    private SQLQueryAdapter generate() {
        QuestDBTable table = globalState.getSchema().getRandomTable();
        List<QuestDBColumn> columns = table.getRandomNonEmptyColumnSubset();
        buildInsertInto(table.getName(), columns);
        QuestDBErrors.addInsertErrors(errors);
        return new SQLQueryAdapter(sb.toString(), errors);
    }

    public static SQLQueryAdapter getQuery(QuestDBGlobalState globalState) {
        return new QuestDBInsertGenerator(globalState).generate();
    }

    @Override
    protected void insertColumns(List<QuestDBColumn> columns) {
        sb.append("(");
        for (int nrColumn = 0; nrColumn < columns.size(); nrColumn++) {
            if (nrColumn != 0) {
                sb.append(", ");
            }
            insertValue(columns.get(nrColumn));
        }
        sb.append(")");
    }

    @Override
    protected void insertValue(QuestDBColumn questDBColumn) {
        sb.append(QuestDBToStringVisitor.asString(new QuestDBExpressionGenerator(globalState).generateConstant()));
    }
}
