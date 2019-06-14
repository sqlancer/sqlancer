package lama.mysql.gen;

import lama.Query;
import lama.QueryAdapter;
import lama.Randomly;
import lama.mysql.MySQLSchema.MySQLTable;

public class MySQLOptimize {

	private final MySQLTable randomTable;
	private final StringBuilder sb = new StringBuilder();

	public MySQLOptimize(MySQLTable randomTable) {
		this.randomTable = randomTable;
	}

	public static Query optimize(MySQLTable randomTable) {
		return new MySQLOptimize(randomTable).optimize();
	}

	private Query optimize() {
		sb.append("OPTIMIZE");
		if (Randomly.getBoolean()) {
			sb.append(" ");
			sb.append(Randomly.fromOptions("NO_WRITE_TO_BINLOG", "LOCAL"));
		}
		sb.append(" TABLE ");
		sb.append(randomTable.getName());
		return new QueryAdapter(sb.toString());
	}

}
