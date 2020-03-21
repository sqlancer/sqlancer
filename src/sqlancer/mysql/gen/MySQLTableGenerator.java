package sqlancer.mysql.gen;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Query;
import sqlancer.QueryAdapter;
import sqlancer.Randomly;
import sqlancer.mysql.MySQLSchema;
import sqlancer.mysql.MySQLSchema.MySQLTable.MySQLEngine;
import sqlancer.sqlite3.gen.SQLite3Common;

public class MySQLTableGenerator {

	private final StringBuilder sb = new StringBuilder();
	private boolean allowPrimaryKey;
	private boolean setPrimaryKey;
	private final String tableName;
	private final Randomly r;
	private int columnId;
	private boolean tableHasNullableColumn;
	private MySQLEngine engine;
	private int keysSpecified;
	private List<String> columns = new ArrayList<>();
	private MySQLSchema schema;

	public MySQLTableGenerator(String tableName, Randomly r, MySQLSchema schema) {
		this.tableName = tableName;
		this.r = r;
		this.schema = schema;
		allowPrimaryKey = Randomly.getBoolean();
	}

	public static Query generate(String tableName, Randomly r, MySQLSchema schema) {
		return new MySQLTableGenerator(tableName, r, schema).create();
	}

	private Query create() {
		List<String> errors = new ArrayList<>();

		sb.append("CREATE");
		if (Randomly.getBoolean()) {
//			sb.append(" TEMPORARY"); // FIXME support temporary tables in the schema
		}
		sb.append(" TABLE");
		if (Randomly.getBoolean()) {
			sb.append(" IF NOT EXISTS");
		}
		sb.append(" " + tableName);
		if (Randomly.getBoolean() && !schema.getDatabaseTables().isEmpty()) {
			sb.append(" LIKE ");
			sb.append(schema.getRandomTable().getName());
			return new QueryAdapter(sb.toString());
		} else {
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
			appendPartitionOptions();
			if ((tableHasNullableColumn || setPrimaryKey) && (engine == MySQLEngine.CSV)) {
				return new QueryAdapter(sb.toString()) {
					public boolean execute(java.sql.Connection con) throws java.sql.SQLException {

						try {
							super.execute(con);
							throw new AssertionError("expected error");
						} catch (SQLException e) {
							if (e.getMessage()
									.startsWith("The storage engine for the table doesn't support nullable columns")) {
								// ignore
							} else if (e.getMessage().startsWith("Too many keys specified; max 0 keys allowed")) {
								// ignore
							} else if (shouldIgnoreCommon(e)) {
								// ignore
							} else {
								throw e;
							}
						}
						return false;

					};
				};
			} else if ((tableHasNullableColumn || keysSpecified > 1) && engine == MySQLEngine.ARCHIVE) {
				errors.add("Too many keys specified; max 1 keys allowed");
				errors.add("Table handler doesn't support NULL in given index");
				errors.add("Got error -1 - 'Unknown error -1' from storage engine");
				addCommonErrors(errors);
				return new QueryAdapter(sb.toString(), errors);
			}
			addCommonErrors(errors);
			return new QueryAdapter(sb.toString(), errors);
		}

	}
	
	private void addCommonErrors(List<String> list) {
		list.add("The storage engine for the table doesn't support");
		list.add("doesn't have this option");
		list.add("must include all columns");
		list.add("not allowed type for this type of partitioning");
		list.add("doesn't support BLOB/TEXT columns");
		list.add("A BLOB field is not allowed in partition function");
	}

	private boolean shouldIgnoreCommon(Exception e) {
		return e.getMessage().contains("The storage engine for the table doesn't support")
				|| e.getMessage().contains("doesn't have this option")
				|| e.getMessage().contains("must include all columns")
				|| e.getMessage().contains("not allowed type for this type of partitioning")
				|| e.getMessage().contains("doesn't support BLOB/TEXT columns")
				|| e.getMessage().contains("A BLOB field is not allowed in partition function");
	}

	private enum PartitionOptions {
		HASH, KEY
	}

	private void appendPartitionOptions() {
		if (engine != MySQLEngine.INNO_DB) {
			return;
		}
		if (Randomly.getBoolean()) {
			return;
		}
		sb.append(" PARTITION BY");
		switch (Randomly.fromOptions(PartitionOptions.values())) {
		case HASH:
			if (Randomly.getBoolean()) {
				sb.append(" LINEAR");
			}
			sb.append(" HASH(");
			// TODO: consider arbitrary expressions
			// MySQLExpression expr =
			// MySQLRandomExpressionGenerator.generateRandomExpression(Collections.emptyList(),
			// null, r);
//			sb.append(MySQLVisitor.asString(expr));
			sb.append(Randomly.fromList(columns));
			sb.append(")");
			break;
		case KEY:
			if (Randomly.getBoolean()) {
				sb.append(" LINEAR");
			}
			sb.append(" KEY");
			if (Randomly.getBoolean()) {
				sb.append(" ALGORITHM=");
				sb.append(Randomly.fromOptions(1, 2));
			}
			sb.append(" (");
			sb.append(Randomly.nonEmptySubset(columns).stream().collect(Collectors.joining(", ")));
			sb.append(")");
			break;
		}
	}

	private enum TableOptions {
		AUTO_INCREMENT, AVG_ROW_LENGTH, CHECKSUM, COMPRESSION, DELAY_KEY_WRITE, /* ENCRYPTION, */ ENGINE, INSERT_METHOD,
		KEY_BLOCK_SIZE, MAX_ROWS, MIN_ROWS, PACK_KEYS, STATS_AUTO_RECALC, STATS_PERSISTENT, STATS_SAMPLE_PAGES;
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
			case COMPRESSION:
				sb.append("COMPRESSION = '");
				sb.append(Randomly.fromOptions("ZLIB", "LZ4", "NONE"));
				sb.append("'");
				break;
			case DELAY_KEY_WRITE:
				sb.append("DELAY_KEY_WRITE = ");
				sb.append(Randomly.fromOptions(0, 1));
				break;
			case ENGINE:
				// FEDERATED: java.sql.SQLSyntaxErrorException: Unknown storage engine
				// 'FEDERATED'
				// "NDB": java.sql.SQLSyntaxErrorException: Unknown storage engine 'NDB'
				// "EXAMPLE": java.sql.SQLSyntaxErrorException: Unknown storage engine 'EXAMPLE'
				// "MERGE": java.sql.SQLException: Table 't0' is read only
				String fromOptions = Randomly.fromOptions("InnoDB", "MyISAM", "MEMORY", "HEAP", "CSV", "ARCHIVE");
				this.engine = MySQLEngine.get(fromOptions);
				sb.append("ENGINE = " + fromOptions);
				break;
//			case ENCRYPTION:
//				sb.append("ENCRYPTION = '");
//				sb.append(Randomly.fromOptions("Y", "N"));
//				sb.append("'");
//				break;
			case INSERT_METHOD:
				sb.append("INSERT_METHOD = ");
				sb.append(Randomly.fromOptions("NO", "FIRST", "LAST"));
				break;
			case KEY_BLOCK_SIZE:
				sb.append("KEY_BLOCK_SIZE = ");
				sb.append(r.getPositiveInteger());
				break;
			case MAX_ROWS:
				sb.append("MAX_ROWS = " + r.getLong(0, Long.MAX_VALUE));
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
				sb.append("STATS_SAMPLE_PAGES = " + r.getInteger(1, Short.MAX_VALUE));
				break;
			default:
				throw new AssertionError(o);
			}
		}
	}

	private void appendColumn() {
		String columnName = SQLite3Common.createColumnName(columnId);
		columns.add(columnName);
		sb.append(columnName);
		appendColumnDefinition();
		columnId++;
	}

	private enum ColumnOptions {
		NULL_OR_NOT_NULL, UNIQUE, COMMENT, COLUMN_FORMAT, STORAGE, PRIMARY_KEY
	}

	private void appendColumnDefinition() {
		sb.append(" ");
		boolean isTextType = false;
		if (Randomly.getBoolean()) {
			String fromOptions = Randomly.fromOptions("TINYINT", "SMALLINT", "MEDIUMINT", "INT", "BIGINT");
			sb.append(fromOptions);
			if (Randomly.getBoolean()) {
				/* workaround for https://bugs.mysql.com/bug.php?id=95954 */
				if (!fromOptions.contentEquals("BIGINT")) {
					sb.append(" UNSIGNED");
				}
			}
		} else {
			isTextType = true;
			sb.append(Randomly.fromOptions("VARCHAR(5)", "TINYTEXT", "TEXT", "MEDIUMTEXT", "LONGTEXT"));
		}
		sb.append(" ");
		// TODO: this was commented out since it makes the implementation of LIKE more
		// difficult
//		if (Randomly.getBoolean()) {
//			sb.append(" ZEROFILL");
//		}
		boolean isNull = false;
		boolean columnHasPrimaryKey = false;

		List<ColumnOptions> columnOptions = Randomly.subset(ColumnOptions.values());
		if (!columnOptions.contains(ColumnOptions.NULL_OR_NOT_NULL)) {
			tableHasNullableColumn = true;
		}
		if (isTextType) {
			// TODO: restriction due to the limited key length
			columnOptions.remove(ColumnOptions.PRIMARY_KEY);
			columnOptions.remove(ColumnOptions.UNIQUE);
		}
		for (ColumnOptions o : columnOptions) {
			sb.append(" ");
			switch (o) {
			case NULL_OR_NOT_NULL:
				// PRIMARY KEYs cannot be NULL
				if (!columnHasPrimaryKey) {
					if (Randomly.getBoolean()) {
						sb.append("NULL");
					}
					tableHasNullableColumn = true;
					isNull = true;
				} else {
					sb.append("NOT NULL");
				}
				break;
			case UNIQUE:
				sb.append("UNIQUE");
				keysSpecified++;
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
