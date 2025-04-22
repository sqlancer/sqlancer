package sqlancer.oxla.schema;

import sqlancer.SQLConnection;
import sqlancer.common.DBMSCommon;
import sqlancer.common.schema.TableIndex;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public final class OxlaIndex extends TableIndex {

    private OxlaIndex(String indexName) {
        super(indexName);
    }

    public static OxlaIndex create(String indexName) {
        return new OxlaIndex(indexName);
    }

    static List<OxlaIndex> getIndexes(SQLConnection con, String tableName) throws SQLException {
        List<OxlaIndex> indexes = new ArrayList<>();
        Statement statement = con.createStatement();
        ResultSet rs = statement.executeQuery(String
                .format("SELECT indexname FROM pg_indexes WHERE tablename='%s' ORDER BY indexname;", tableName));
        while (rs.next()) {
            String indexName = rs.getString("indexname");
            if (DBMSCommon.matchesIndexName(indexName)) {
                indexes.add(create(indexName));
            }
        }
        return indexes;
    }
}
