package sqlancer.mysql.gen.tblmaintenance;

import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.mysql.MySQLGlobalState;
import sqlancer.mysql.MySQLSchema.MySQLTable;

/**
 * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/checksum-table.html">CHECKSUM TABLE Statement</a>
 */
public class MySQLChecksum {

    private final List<MySQLTable> tables;
    private final StringBuilder sb = new StringBuilder();

    public MySQLChecksum(List<MySQLTable> tables) {
        this.tables = tables;
    }

    public static SQLQueryAdapter checksum(MySQLGlobalState globalState) {
        return new MySQLChecksum(globalState.getSchema().getDatabaseTablesRandomSubsetNotEmpty()).checksum();
    }

    // CHECKSUM TABLE tbl_name [, tbl_name] ... [QUICK | EXTENDED]
    private SQLQueryAdapter checksum() {
        sb.append("CHECKSUM TABLE ");
        sb.append(tables.stream().map(t -> t.getName()).collect(Collectors.joining(", ")));
        if (Randomly.getBoolean()) {
            sb.append(" ");
            sb.append(Randomly.fromOptions("QUICK", "EXTENDED"));
        }
        return new SQLQueryAdapter(sb.toString());
    }

}
