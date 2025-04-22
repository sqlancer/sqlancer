package sqlancer.oxla.schema;

import sqlancer.SQLConnection;
import sqlancer.common.schema.AbstractSchema;
import sqlancer.oxla.OxlaGlobalState;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class OxlaSchema extends AbstractSchema<OxlaGlobalState, OxlaTable> {
    private final String databaseName;

    public OxlaSchema(List<OxlaTable> databaseTables, String databaseName) {
        super(databaseTables);
        this.databaseName = databaseName;
    }

    public static OxlaSchema fromConnection(SQLConnection connection, String databaseName) throws SQLException {
        try {
            List<OxlaTable> databaseTables = new ArrayList<>();
            Statement statement = connection.createStatement();
            ResultSet rs = statement.executeQuery(
                    "SELECT table_name FROM information_schema.tables WHERE table_schema='public' OR table_schema LIKE 'pg_temp_%' ORDER BY table_name;");
            while (rs.next()) {
                String tableName = rs.getString("table_name");
                List<OxlaColumn> databaseColumns = OxlaTable.getTableColumns(connection, tableName);
//                List<OxlaIndex> indexes = OxlaIndex.getIndexes(connection, tableName);
                OxlaTable table = new OxlaTable(tableName, databaseColumns, null);
                for (OxlaColumn column : databaseColumns) {
                    column.setTable(table);
                }
                databaseTables.add(table);
            }
            return new OxlaSchema(databaseTables, databaseName);
        } catch (SQLIntegrityConstraintViolationException e) {
            throw new AssertionError(e);
        }
    }


    public String getDatabaseName() {
        return databaseName;
    }
}

