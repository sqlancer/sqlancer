package sqlancer.hsqldb.gen;

import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;

import sqlancer.Randomly;
import sqlancer.common.gen.AbstractTableGenerator;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.hsqldb.HSQLDBProvider;
import sqlancer.hsqldb.HSQLDBSchema;

public class HSQLDBTableGenerator extends AbstractTableGenerator<HSQLDBSchema.HSQLDBColumn> {

    private HSQLDBProvider.HSQLDBGlobalState globalState;
    private String tableName;

    public HSQLDBTableGenerator() {
        this.canAffectSchema = true;
    }

    public SQLQueryAdapter getQuery(HSQLDBProvider.HSQLDBGlobalState globalState, @Nullable String tableName) {
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
    }

    @Override
    protected void appendColumnDefinition(HSQLDBSchema.HSQLDBColumn column) {
        sb.append(column.getName());
        sb.append(" ");
        sb.append(column.getType().getType().name());
        if (column.getType().getSize() > 0) {
            // Cannot specify size for non composite data types
            sb.append("(");
            sb.append(column.getType().getSize());
            sb.append(")");
        }
    }

    private static List<HSQLDBSchema.HSQLDBColumn> getNewColumns() {
        List<HSQLDBSchema.HSQLDBColumn> columns = new ArrayList<>();
        for (int i = 0; i < Randomly.smallNumber() + 1; i++) {
            String columnName = String.format("c%d", i);
            HSQLDBSchema.HSQLDBCompositeDataType columnType = HSQLDBSchema.HSQLDBCompositeDataType
                    .getRandomWithoutNull();
            columns.add(new HSQLDBSchema.HSQLDBColumn(columnName, null, columnType));
        }
        return columns;
    }
}
