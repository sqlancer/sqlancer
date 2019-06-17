package lama.mysql.gen;

import java.util.List;

import lama.Query;
import lama.QueryAdapter;
import lama.Randomly;
import lama.sqlite3.gen.SQLite3Common;

public class MySQLTableGenerator {

	private final StringBuilder sb = new StringBuilder();
	private boolean allowPrimaryKey;
	private boolean setPrimaryKey;
	private final String tableName;
	private final Randomly r;
	private int columnId;

	public MySQLTableGenerator(String tableName, Randomly r) {
		this.tableName = tableName;
		this.r = r;
		allowPrimaryKey = Randomly.getBoolean();
	}

	public static Query generate(String tableName, Randomly r) {
		return new MySQLTableGenerator(tableName, r).create();
	}

	private Query create() {
		sb.append("CREATE");
		if (Randomly.getBoolean()) {
//			sb.append(" TEMPORARY"); // FIXME support temporary tables in the schema
		}
		sb.append(" TABLE");
		if (Randomly.getBoolean()) {
			sb.append(" IF NOT EXISTS");
		}
		sb.append(" " + tableName);
		sb.append("(");
		for (int i = 0; i < 3 + Randomly.smallNumber(); i++) {
			if (i != 0) {
				sb.append(", ");
			}
			appendColumn();
		}
		sb.append(")");
		sb.append(" ");
		appendTableOptions();
		return new QueryAdapter(sb.toString());

	}

	private enum TableOptions {
		AUTO_INCREMENT, AVG_ROW_LENGTH, CHECKSUM, ENGINE, MIN_ROWS, PACK_KEYS, STATS_AUTO_RECALC, STATS_PERSISTENT, STATS_SAMPLE_PAGES;
	}

	private void appendTableOptions() {
		List<TableOptions> options = Randomly.subset(TableOptions.values());
		int i = 0;
		for (TableOptions o : options) {
			if (i++ != 0) {
				sb.append(", ");
			}
			switch (o) {
			case AUTO_INCREMENT:
				sb.append("AUTO_INCREMENT = " + r.getPositiveInteger());
				break;
			case AVG_ROW_LENGTH:
				sb.append("AVG_ROW_LENGTH = " + r.getPositiveInteger());
				break;
			case CHECKSUM:
				sb.append("CHECKSUM = 1");
				break;
			case ENGINE:
				// FEDERATED: java.sql.SQLSyntaxErrorException: Unknown storage engine 'FEDERATED'
				// "ARCHIVE": java.sql.SQLSyntaxErrorException: Too many keys specified; max 1 keys allowed
				//  "NDB": java.sql.SQLSyntaxErrorException: Unknown storage engine 'NDB'
				// "CSV": java.sql.SQLSyntaxErrorException: Too many keys specified; max 0 keys allowed
				// "EXAMPLE": java.sql.SQLSyntaxErrorException: Unknown storage engine 'EXAMPLE'
				// "MERGE": java.sql.SQLException: Table 't0' is read only
				sb.append("ENGINE = " + Randomly.fromOptions("InnoDB", "MyISAM", "MEMORY", "HEAP"));
				break;
			case MIN_ROWS:
				sb.append("MIN_ROWS = " + r.getLong(1, Long.MAX_VALUE));
				break;
			case PACK_KEYS:
				sb.append("PACK_KEYS = " + Randomly.fromOptions("1", "0", "DEFAULT"));
				break;
			case STATS_AUTO_RECALC:
				sb.append("STATS_AUTO_RECALC = " + Randomly.fromOptions("1", "0", "DEFAULT"));
				break;
			case STATS_PERSISTENT:
				sb.append("STATS_PERSISTENT = " + Randomly.fromOptions("1", "0", "DEFAULT"));
				break;
			case STATS_SAMPLE_PAGES:
				sb.append("STATS_SAMPLE_PAGES = " + r.getInteger(0, Short.MAX_VALUE));
				break;
			default:
				throw new AssertionError(o);
			}
		}
	}

	private void appendColumn() {
		String columnName = SQLite3Common.createColumnName(columnId);
		sb.append(columnName);
		appendColumnDefinition();
		columnId++;
	}
	
	private enum ColumnOptions {
		NULL_OR_NOT_NULL, UNIQUE, COMMENT, COLUMN_FORMAT, STORAGE, PRIMARY_KEY
	}

	private void appendColumnDefinition() {
		sb.append(" INT ");
		boolean isNull = false;
		boolean columnHasPrimaryKey = false;

		
		List<ColumnOptions> columnOptions = Randomly.subset(ColumnOptions.values());
		for (ColumnOptions o : columnOptions) {
			sb.append(" ");
			switch (o) {
			case NULL_OR_NOT_NULL:
				// PRIMARY KEYs cannot be NULL
				if (Randomly.getBoolean() && !columnHasPrimaryKey) {
					sb.append("NULL");
					isNull = true;
				} else {
					sb.append("NOT NULL");
 				}
				break;
			case UNIQUE:
				sb.append("UNIQUE");
				if (Randomly.getBoolean()) {
					sb.append(" KEY");
				}
				break;
			case COMMENT:
				// TODO: generate randomly
				sb.append(String.format("COMMENT '%s' ", "asdf"));
				break;
			case COLUMN_FORMAT:
				sb.append("COLUMN_FORMAT ");
				sb.append(Randomly.fromOptions("FIXED", "DYNAMIC", "DEFAULT"));
				break;
			case STORAGE:
				sb.append("STORAGE ");
				sb.append(Randomly.fromOptions("DISK", "MEMORY"));
				break;
			case PRIMARY_KEY:
				// PRIMARY KEYs cannot be NULL
				if (allowPrimaryKey && !setPrimaryKey && !isNull) {
					sb.append("PRIMARY KEY");
					setPrimaryKey = true;
					columnHasPrimaryKey = true;
				}
				break;
			}
		}
		
	}

}
