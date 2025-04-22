package sqlancer.oxla.schema;

import sqlancer.SQLConnection;
import sqlancer.common.schema.AbstractRelationalTable;
import sqlancer.oxla.OxlaGlobalState;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class OxlaTable extends AbstractRelationalTable<OxlaColumn, OxlaIndex, OxlaGlobalState> {
    public OxlaTable(String name, List<OxlaColumn> columns, List<OxlaIndex> indexes) {
        super(name, columns, indexes, false);
    }

    protected static List<OxlaColumn> getTableColumns(SQLConnection con, String tableName) throws SQLException {
        List<OxlaColumn> columns = new ArrayList<>();
        Statement statement = con.createStatement();
        ResultSet rs = statement.executeQuery("select column_name, data_type from information_schema.columns where table_name = '" + tableName + "' ORDER BY column_name");
        while (rs.next()) {
            String columnName = rs.getString("column_name");
            String dataType = rs.getString("data_type");
            columns.add(new OxlaColumn(columnName, OxlaDataType.fromString(dataType)));
        }
        return columns;
    }
}
