package sqlancer.influxdb;

import java.util.*;
import okhttp3.OkHttpClient;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.common.schema.AbstractSchema;
import sqlancer.common.schema.AbstractTableColumn;
import sqlancer.common.schema.AbstractRelationalTable;

public class InfluxDBSchema extends AbstractSchema<SQLConnection, InfluxDBSchema.InfluxDBTable> {

    public enum InfluxDBDataType {
        INTEGER, FLOAT, STRING, BOOLEAN, TIME;

        public static InfluxDBDataType getRandomWithoutTime() {
            return Randomly.fromOptions(INTEGER, FLOAT, STRING, BOOLEAN);
        }
    }

    public static class InfluxDBCompositeDataType {
        private final InfluxDBDataType dataType;

        public InfluxDBCompositeDataType(InfluxDBDataType dataType) {
            this.dataType = dataType;
        }

        public InfluxDBDataType getPrimitiveDataType() {
            return dataType;
        }

        @Override
        public String toString() {
            return dataType.name();
        }
    }

    public static class InfluxDBColumn extends AbstractTableColumn<InfluxDBTable, InfluxDBDataType> {
        private final boolean isTag;

        public InfluxDBColumn(String name, InfluxDBDataType columnType, boolean isTag) {
            super(name, null, columnType);
            this.isTag = isTag;
        }

        public boolean isTag() {
            return isTag;
        }
    }

    public static class InfluxDBTable extends AbstractRelationalTable<InfluxDBColumn, Object, SQLConnection> {
        public InfluxDBTable(String tableName, List<InfluxDBColumn> columns) {
            super(tableName, columns, Collections.emptyList(), false);
        }
    }

    public InfluxDBSchema(List<InfluxDBTable> databaseTables) {
        super(databaseTables);
    }

    private static InfluxDBDataType getColumnType(String typeString) {
        switch (typeString) {
            case "integer":
                return InfluxDBDataType.INTEGER;
            case "float":
                return InfluxDBDataType.FLOAT;
            case "string":
                return InfluxDBDataType.STRING;
            case "boolean":
                return InfluxDBDataType.BOOLEAN;
            case "time":
                return InfluxDBDataType.TIME;
            default:
                throw new AssertionError(typeString);
        }
    }

    public static InfluxDBSchema fromConnection(SQLConnection con, String databaseName) {
        InfluxDB influxDB = InfluxDBFactory.connect("http://localhost:8086", "root", "root");
        Query query = new Query("SHOW FIELD KEYS", databaseName);
        QueryResult result = influxDB.query(query);

        List<InfluxDBTable> databaseTables = new ArrayList<>();
        for (QueryResult.Series series : result.getResults().get(0).getSeries()) {
            String tableName = series.getName();
            List<InfluxDBColumn> columns = new ArrayList<>();
            for (List<Object> values : series.getValues()) {
                String columnName = (String) values.get(0);
                String columnTypeString = (String) values.get(1);
                InfluxDBDataType columnType = getColumnType(columnTypeString);
                InfluxDBColumn column = new InfluxDBColumn(columnName, columnType, false);
                columns.add(column);
            }
            InfluxDBTable table = new InfluxDBTable(tableName, columns);
            databaseTables.add(table);
        }

        return new InfluxDBSchema(databaseTables);
    }
}