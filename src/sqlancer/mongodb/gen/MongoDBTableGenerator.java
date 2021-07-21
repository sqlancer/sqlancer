package sqlancer.mongodb.gen;

import java.util.ArrayList;
import java.util.List;

import sqlancer.Randomly;
import sqlancer.mongodb.MongoDBProvider.MongoDBGlobalState;
import sqlancer.mongodb.MongoDBQueryAdapter;
import sqlancer.mongodb.MongoDBSchema.MongoDBColumn;
import sqlancer.mongodb.MongoDBSchema.MongoDBDataType;
import sqlancer.mongodb.MongoDBSchema.MongoDBTable;
import sqlancer.mongodb.query.MongoDBCreateTableQuery;

public class MongoDBTableGenerator {

    private MongoDBTable table;
    private final List<MongoDBColumn> columnsToBeAdded = new ArrayList<>();
    private final MongoDBGlobalState state;

    public MongoDBTableGenerator(MongoDBGlobalState state) {
        this.state = state;
    }

    public MongoDBQueryAdapter getQuery(MongoDBGlobalState globalState) {
        String tableName = globalState.getSchema().getFreeTableName();
        MongoDBCreateTableQuery createTableQuery = new MongoDBCreateTableQuery(tableName);
        table = new MongoDBTable(tableName, columnsToBeAdded, false);
        for (int i = 0; i < Randomly.smallNumber() + 1; i++) {
            String columnName = String.format("c%d", i);
            MongoDBDataType type = createColumn(columnName);
            if (globalState.getDbmsSpecificOptions().testValidation) {
                createTableQuery.addValidation(columnName, type.getBsonType());
            }
        }
        globalState.addTable(table);
        return createTableQuery;
    }

    private MongoDBDataType createColumn(String columnName) {
        MongoDBDataType columnType = MongoDBDataType.getRandom(state);
        MongoDBColumn newColumn = new MongoDBColumn(columnName, columnType, false, false);
        newColumn.setTable(table);
        columnsToBeAdded.add(newColumn);
        return columnType;
    }

    public String getTableName() {
        return table.getName();
    }

    public MongoDBTable getGeneratedTable() {
        return table;
    }
}
