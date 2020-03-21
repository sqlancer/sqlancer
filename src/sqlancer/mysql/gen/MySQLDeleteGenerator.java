package sqlancer.mysql.gen;

import java.util.Arrays;

import sqlancer.Query;
import sqlancer.QueryAdapter;
import sqlancer.Randomly;
import sqlancer.mysql.MySQLSchema.MySQLTable;

public class MySQLDeleteGenerator {

	private MySQLTable randomTable;
	private Randomly r;
	private final StringBuilder sb = new StringBuilder();

	public MySQLDeleteGenerator(MySQLTable randomTable, Randomly r) {
		this.randomTable = randomTable;
		this.r = r;
	}

	public static Query delete(MySQLTable randomTable, Randomly r) {
		return new MySQLDeleteGenerator(randomTable, r).generate();
	}

	private Query generate() {
		sb.append("DELETE");
		if (Randomly.getBoolean()) {
			sb.append(" LOW_PRIORITY");
		}
		if (Randomly.getBoolean()) {
			sb.append(" QUICK");
		}
		if (Randomly.getBoolean()) {
			sb.append(" IGNORE");
		}
		// TODO: support partitions
		sb.append(" FROM ");
		sb.append(randomTable.getName());
		if (Randomly.getBoolean()) {
			sb.append(" WHERE ");
			sb.append(MySQLRandomExpressionGenerator.generateRandomExpressionString(randomTable.getColumns(), null, r));
		}
		if (Randomly.getBoolean()) {
			sb.append(" ");
			sb.append(r.getLong(0, Long.MAX_VALUE));
		}

		// TODO: support ORDER BY
		return new QueryAdapter(sb.toString(), Arrays.asList("doesn't have this option", "Truncated incorrect DOUBLE value" /* ignore as a workaround for https://bugs.mysql.com/bug.php?id=95997 */));
	}

}
