package lama.mysql.gen.tblmaintenance;

import lama.Query;
import lama.QueryAdapter;
import lama.Randomly;
import lama.mysql.MySQLSchema.MySQLTable;

public class MySQLRepair {

	private final MySQLTable randomTable;
	private final StringBuilder sb = new StringBuilder();
	
	public MySQLRepair(MySQLTable randomTable) {
		this.randomTable = randomTable;
	}

	public static Query repair(MySQLTable randomTable) {
		return new MySQLRepair(randomTable).repair();
	}

	private Query repair() {
		sb.append("REPAIR");
		if (Randomly.getBoolean()) {
			sb.append(" ");
			sb.append(Randomly.fromOptions("NO_WRITE_TO_BINLOG", "LOCAL"));
		}
		sb.append(" TABLE ");
		sb.append(randomTable.getName());
		if (Randomly.getBoolean()) {
			sb.append(" QUICK");
		}
		if (Randomly.getBoolean()) {
			sb.append(" EXTENDED");
		}
		if (Randomly.getBoolean()) {
			sb.append(" USE_FRM");
		}
		return new QueryAdapter(sb.toString());
	}

}
