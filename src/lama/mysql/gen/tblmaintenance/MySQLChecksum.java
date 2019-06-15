package lama.mysql.gen.tblmaintenance;

import java.util.List;
import java.util.stream.Collectors;

import lama.Query;
import lama.QueryAdapter;
import lama.Randomly;
import lama.mysql.MySQLSchema.MySQLTable;

/**
 * @see https://dev.mysql.com/doc/refman/8.0/en/checksum-table.html
 */
public class MySQLChecksum {

	private final List<MySQLTable> tables;
	private final StringBuilder sb = new StringBuilder();

	public MySQLChecksum(List<MySQLTable> tables) {
		this.tables = tables;
	}

	public static Query checksum(List<MySQLTable> tables) {
		return new MySQLChecksum(tables).checksum();
	}

	// CHECKSUM TABLE tbl_name [, tbl_name] ... [QUICK | EXTENDED]
	private Query checksum() {
		sb.append("CHECKSUM TABLE ");
		sb.append(tables.stream().map(t -> t.getName()).collect(Collectors.joining(", ")));
		if (Randomly.getBoolean()) {
			sb.append(" ");
			sb.append(Randomly.fromOptions("QUICK", "EXTENDED"));
		}
		return new QueryAdapter(sb.toString());
	}

}
