package sqlancer.questdb.gen;

import java.util.List;
import java.util.stream.Collectors;

import sqlancer.common.gen.AbstractInsertGenerator;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.schema.AbstractTableColumn;
import sqlancer.questdb.QuestDBErrors;
import sqlancer.questdb.QuestDBProvider.QuestDBGlobalState;
import sqlancer.questdb.QuestDBSchema.QuestDBColumn;
import sqlancer.questdb.QuestDBSchema.QuestDBTable;
import sqlancer.questdb.ast.QuestDBConstants;

public class QuestDBInsertGenerator extends AbstractInsertGenerator<QuestDBColumn> {

    private final QuestDBGlobalState globalState;

    private final ExpectedErrors errors = new ExpectedErrors();

    public QuestDBInsertGenerator(QuestDBGlobalState globalState) {
        this.globalState = globalState;
    }

    private SQLQueryAdapter generate() {
        sb.append("INSERT INTO ");
        QuestDBTable table = globalState.getSchema().getRandomTable();
        List<QuestDBColumn> columns = table.getRandomNonEmptyColumnSubset();
        sb.append('\'').append(table.getName()).append('\'');
        sb.append(" (");
        sb.append(columns.stream().map(AbstractTableColumn::getName).collect(Collectors.joining(", ")));
        sb.append(") VALUES ");
        insertColumns(columns);
        QuestDBErrors.addInsertErrors(errors);
        return new SQLQueryAdapter(sb.toString(), errors);
    }

    public static SQLQueryAdapter getQuery(QuestDBGlobalState globalState) {
        return new QuestDBInsertGenerator(globalState).generate();
    }

    @Override
    protected void insertColumns(List<QuestDBColumn> columns) {
        sb.append('(');
        for (int nrColumn = 0, n = columns.size(); nrColumn < n; nrColumn++) {
            insertValue(columns.get(nrColumn));
            if (nrColumn + 1 < n) {
                sb.append(", ");
            }
        }
        sb.append(')');
    }

    @Override
    protected void insertValue(QuestDBColumn questDBColumn) {
        sb.append(QuestDBConstants.createRandomQuestDBConstant(globalState.getRandomly()));
    }
}
