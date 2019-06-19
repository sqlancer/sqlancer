package lama.mysql.gen;

import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

import lama.Query;
import lama.QueryAdapter;
import lama.Randomly;
import lama.mysql.MySQLSchema;
import lama.mysql.MySQLSchema.MySQLTable;

public class MySQLAlterTable {

	private final MySQLSchema schema;
	private final Randomly r;
	private final StringBuilder sb = new StringBuilder();

	public MySQLAlterTable(MySQLSchema newSchema, Randomly r) {
		this.schema = newSchema;
		this.r = r;
	}

	public static Query create(MySQLSchema newSchema, Randomly r) {
		return new MySQLAlterTable(newSchema, r).create();
	}

	private enum Action {
		ALGORITHM, CHECKSUM, COMPRESSION, DISABLE_ENABLE_KEYS, FORCE, DELAY_KEY_WRITE, INSERT_METHOD, ROW_FORMAT,
		STATS_AUTO_RECALC, STATS_PERSISTENT, PACK_KEYS
	}

	private Query create() {
		sb.append("ALTER TABLE ");
		MySQLTable table = schema.getRandomTable();
		sb.append(table.getName());
		sb.append(" ");
		List<Action> actions = Randomly.subset(Action.values());
		int i = 0;
		for (Action a : actions) {
			if (i++ != 0) {
				sb.append(", ");
			}
			switch (a) {
			case ALGORITHM:
				sb.append("ALGORITHM ");
				sb.append(Randomly.fromOptions("INSTANT", "INPLACE", "COPY", "DEFAULT"));
				break;
			case CHECKSUM:
				sb.append("CHECKSUM ");
				sb.append(Randomly.fromOptions(0, 1));
				break;
			case COMPRESSION:
				sb.append("COMPRESSION ");
				sb.append("'");
				sb.append(Randomly.fromOptions("ZLIB", "LZ4", "NONE"));
				sb.append("'");
				break;
			case DELAY_KEY_WRITE:
				sb.append("DELAY_KEY_WRITE ");
				sb.append(Randomly.fromOptions(0, 1));
				break;
			case DISABLE_ENABLE_KEYS:
				sb.append(Randomly.fromOptions("DISABLE", "ENABLE"));
				sb.append(" KEYS");
				break;
			case FORCE:
				sb.append("FORCE");
				break;
			case INSERT_METHOD:
				sb.append("INSERT_METHOD ");
				sb.append(Randomly.fromOptions("NO", "FIRST", "LAST"));
				break;
			case ROW_FORMAT:
				sb.append("ROW_FORMAT ");
				sb.append(Randomly.fromOptions("DEFAULT", "DYNAMIC", "FIXED", "COMPRESSED", "REDUNDANT", "COMPACT"));
				break;
			case STATS_AUTO_RECALC:
				sb.append("STATS_AUTO_RECALC ");
				sb.append(Randomly.fromOptions(0, 1, "DEFAULT"));
				break;
			case STATS_PERSISTENT:
				sb.append("STATS_PERSISTENT ");
				sb.append(Randomly.fromOptions(0, 1, "DEFAULT"));
				break;
			case PACK_KEYS:
				sb.append("PACK_KEYS ");
				sb.append(Randomly.fromOptions(0, 1, "DEFAULT"));
				break;
			}
		}
		if (Randomly.getBooleanWithSmallProbability()) {
			if (i != 0) {
				sb.append(", ");
			}
			// should be given as last option
			sb.append(" ORDER BY ");
			sb.append(table.getRandomNonEmptyColumnSubset().stream().map(c -> c.getName())
					.collect(Collectors.joining(", ")));
		}
		// TODO Auto-generated method stub
		return new QueryAdapter(sb.toString()) {
			public void execute(java.sql.Connection con) throws java.sql.SQLException {
				try {
					super.execute(con);
				} catch (SQLException e) {
					if (e.getMessage().contains("does not support the create option")) {
						// ignore
					} else if (e.getMessage().contains("doesn't have this option")) {
						// ignore
					} else if (e.getMessage().contains("is not supported for this operation")) {
						// ignore
					} else {
						throw e;
					}
				}

			};

		};
	}

}
