package lama.mysql.gen.tblmaintenance;

import java.util.List;
import java.util.stream.Collectors;

import lama.Query;
import lama.QueryAdapter;
import lama.Randomly;
import lama.mysql.MySQLSchema.MySQLTable;

public class MySQLOptimize {

	private final List<MySQLTable> tables;
	private final StringBuilder sb = new StringBuilder();

	public MySQLOptimize(List<MySQLTable> tables) {
		this.tables = tables;
	}

	public static Query optimize(List<MySQLTable> tables) {
		return new MySQLOptimize(tables).optimize();
	}

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
