package sqlancer.questdb.gen;

import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;

import sqlancer.Randomly;
import sqlancer.common.gen.AbstractTableGenerator;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.questdb.QuestDBProvider.QuestDBGlobalState;
import sqlancer.questdb.QuestDBSchema.QuestDBColumn;
import sqlancer.questdb.QuestDBSchema.QuestDBCompositeDataType;

public class QuestDBTableGenerator extends AbstractTableGenerator<QuestDBColumn> {

    private QuestDBGlobalState globalState;
    private String tableName;

    public QuestDBTableGenerator() {
        this.canAffectSchema = true;
    }

    public SQLQueryAdapter getQuery(QuestDBGlobalState globalState, @Nullable String tableName) {
        this.globalState = globalState;
        this.tableName = tableName;
        return getStatement();
    }

    @Override
    public void buildStatement() {
        String name = tableName;
        if (name == null) {
            name = globalState.getSchema().getFreeTableName();
        }
        appendCreateTable(name, Randomly.getBoolean());
        appendColumnDefinitions(getNewColumns());
        sb.append(";");
        errors.add("table already exists");
    }

    private static List<QuestDBColumn> getNewColumns() {
        List<QuestDBColumn> columns = new ArrayList<>();
        for (int i = 0; i < Randomly.smallNumber() + 1; i++) {
            String columnName = String.format("c%d", i);
            QuestDBCompositeDataType columnType = QuestDBCompositeDataType.getRandomWithoutNull();
            columns.add(new QuestDBColumn(columnName, columnType, false));
        }
        return columns;
    }
}
