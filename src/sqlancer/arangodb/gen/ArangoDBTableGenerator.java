package sqlancer.arangodb.gen;

import java.util.ArrayList;
import java.util.List;

import sqlancer.Randomly;
import sqlancer.arangodb.ArangoDBProvider;
import sqlancer.arangodb.ArangoDBQueryAdapter;
import sqlancer.arangodb.ArangoDBSchema;
import sqlancer.arangodb.query.ArangoDBCreateTableQuery;

public class ArangoDBTableGenerator {

    private ArangoDBSchema.ArangoDBTable table;
    private final List<ArangoDBSchema.ArangoDBColumn> columnsToBeAdded = new ArrayList<>();

    public ArangoDBQueryAdapter getQuery(ArangoDBProvider.ArangoDBGlobalState globalState) {
        String tableName = globalState.getSchema().getFreeTableName();
        ArangoDBCreateTableQuery createTableQuery = new ArangoDBCreateTableQuery(tableName);
        table = new ArangoDBSchema.ArangoDBTable(tableName, columnsToBeAdded, false);
        for (int i = 0; i < Randomly.smallNumber() + 1; i++) {
            String columnName = String.format("c%d", i);
            createColumn(columnName);
        }
        globalState.addTable(table);
        return createTableQuery;
    }

    private ArangoDBSchema.ArangoDBDataType createColumn(String columnName) {
        ArangoDBSchema.ArangoDBDataType dataType = ArangoDBSchema.ArangoDBDataType.getRandom();
        ArangoDBSchema.ArangoDBColumn newColumn = new ArangoDBSchema.ArangoDBColumn(columnName, dataType, false, false);
        newColumn.setTable(table);
        columnsToBeAdded.add(newColumn);
        return dataType;
    }

    public String getTableName() {
        return table.getName();
    }

    public ArangoDBSchema.ArangoDBTable getGeneratedTable() {
        return table;
    }
}
