package sqlancer.mysql.gen.tblmaintenance;

import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.query.Query;
import sqlancer.common.query.QueryAdapter;
import sqlancer.mysql.MySQLGlobalState;
import sqlancer.mysql.MySQLSchema.MySQLColumn;
import sqlancer.mysql.MySQLSchema.MySQLTable;

/**
 * @see https://dev.mysql.com/doc/refman/8.0/en/analyze-table.html
 */
public class MySQLAnalyzeTable {

    private final List<MySQLTable> tables;
    private final StringBuilder sb = new StringBuilder();
    private final Randomly r;

    public MySQLAnalyzeTable(List<MySQLTable> tables, Randomly r) {
        this.tables = tables;
        this.r = r;
    }

    public static Query analyze(MySQLGlobalState globalState) {
        return new MySQLAnalyzeTable(globalState.getSchema().getDatabaseTablesRandomSubsetNotEmpty(),
                globalState.getRandomly()).generate();
    }

    private Query generate() {
        sb.append("ANALYZE ");
        if (Randomly.getBoolean()) {
            sb.append(Randomly.fromOptions("NO_WRITE_TO_BINLOG", "LOCAL"));
        }
        sb.append(" TABLE ");
        if (Randomly.getBoolean()) {
            analyzeWithoutHistogram();
        } else {
            if (Randomly.getBoolean()) {
                dropHistogram();
            } else {
                updateHistogram();
            }
        }
        return new QueryAdapter(sb.toString());
    }

    // ANALYZE [NO_WRITE_TO_BINLOG | LOCAL]
    // TABLE tbl_name [, tbl_name] ...
    private void analyzeWithoutHistogram() {
        sb.append(tables.stream().map(t -> t.getName()).collect(Collectors.joining(", ")));
    }

    // ANALYZE [NO_WRITE_TO_BINLOG | LOCAL]
    // TABLE tbl_name
    // UPDATE HISTOGRAM ON col_name [, col_name] ...
    // [WITH N BUCKETS]
    private void updateHistogram() {
        MySQLTable table = Randomly.fromList(tables);
        sb.append(table.getName());
        sb.append(" UPDATE HISTOGRAM ON ");
        List<MySQLColumn> columns = table.getRandomNonEmptyColumnSubset();
        sb.append(columns.stream().map(c -> c.getName()).collect(Collectors.joining(", ")));
        if (Randomly.getBoolean()) {
            sb.append(" WITH ");
            sb.append(r.getInteger(1, 1024));
            sb.append(" BUCKETS");
        }
    }

    // ANALYZE [NO_WRITE_TO_BINLOG | LOCAL]
    // TABLE tbl_name
    // DROP HISTOGRAM ON col_name [, col_name] ...
    private void dropHistogram() {
        MySQLTable table = Randomly.fromList(tables);
        sb.append(table.getName());
        sb.append(" DROP HISTOGRAM ON ");
        List<MySQLColumn> columns = table.getRandomNonEmptyColumnSubset();
        sb.append(columns.stream().map(c -> c.getName()).collect(Collectors.joining(", ")));
    }

}
