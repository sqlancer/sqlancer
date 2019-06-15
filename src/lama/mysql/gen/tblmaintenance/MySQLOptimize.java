package lama.mysql.gen.tblmaintenance;

import java.util.List;
import java.util.stream.Collectors;

import lama.Query;
import lama.QueryAdapter;
import lama.Randomly;
import lama.mysql.MySQLSchema.MySQLTable;

/**
 * @see https://dev.mysql.com/doc/refman/8.0/en/optimize-table.html
 */
public class MySQLOptimize {

	private final List<MySQLTable> tables;
	private final StringBuilder sb = new StringBuilder();

	public MySQLOptimize(List<MySQLTable> tables) {
		this.tables = tables;
	}

	public static Query optimize(List<MySQLTable> tables) {
		return new MySQLOptimize(tables).optimize();
	}

	// OPTIMIZE [NO_WRITE_TO_BINLOG | LOCAL]
    // TABLE tbl_name [, tbl_name] ...
	private Query optimize() {
		sb.append("OPTIMIZE");
		if (Randomly.getBoolean()) {
			sb.append(" ");
			sb.append(Randomly.fromOptions("NO_WRITE_TO_BINLOG", "LOCAL"));
		}
		sb.append(" TABLE ");
		sb.append(tables.stream().map(t -> t.getName()).collect(Collectors.joining(", ")));
		return new QueryAdapter(sb.toString());
	}

}
