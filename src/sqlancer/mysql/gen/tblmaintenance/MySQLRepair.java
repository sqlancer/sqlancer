package sqlancer.mysql.gen.tblmaintenance;

import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.mysql.MySQLGlobalState;
import sqlancer.mysql.MySQLSchema.MySQLTable;
import sqlancer.mysql.MySQLSchema.MySQLTable.MySQLEngine;

/**
 * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/repair-table.html">REPAIR TABLE Statement</a>
 */
public class MySQLRepair {

    private final List<MySQLTable> tables;
    private final StringBuilder sb = new StringBuilder();

    public MySQLRepair(List<MySQLTable> tables) {
        this.tables = tables;
    }

    public static SQLQueryAdapter repair(MySQLGlobalState globalState) {
        List<MySQLTable> tables = globalState.getSchema().getDatabaseTablesRandomSubsetNotEmpty();
        for (MySQLTable table : tables) {
            // see https://bugs.mysql.com/bug.php?id=95820
            if (table.getEngine() == MySQLEngine.MY_ISAM) {
                return new SQLQueryAdapter("SELECT 1");
            }
        }
        return new MySQLRepair(tables).repair();
    }

    // REPAIR [NO_WRITE_TO_BINLOG | LOCAL]
    // TABLE tbl_name [, tbl_name] ...
    // [QUICK] [EXTENDED] [USE_FRM]
    private SQLQueryAdapter repair() {
        sb.append("REPAIR");
        if (Randomly.getBoolean()) {
            sb.append(" ");
            sb.append(Randomly.fromOptions("NO_WRITE_TO_BINLOG", "LOCAL"));
        }
        sb.append(" TABLE ");
        sb.append(tables.stream().map(t -> t.getName()).collect(Collectors.joining(", ")));
        if (Randomly.getBoolean()) {
            sb.append(" QUICK");
        }
        if (Randomly.getBoolean()) {
            sb.append(" EXTENDED");
        }
        if (Randomly.getBoolean()) {
            sb.append(" USE_FRM");
        }
        return new SQLQueryAdapter(sb.toString());
    }

}
