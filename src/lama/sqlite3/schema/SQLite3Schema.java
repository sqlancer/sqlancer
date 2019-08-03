package lama.sqlite3.schema;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import lama.Randomly;
import lama.StateToReproduce.SQLite3StateToReproduce;
import lama.sqlite3.SQLite3ToStringVisitor;
import lama.sqlite3.ast.SQLite3Constant;
import lama.sqlite3.schema.SQLite3Schema.Table;
import lama.sqlite3.schema.SQLite3Schema.Column.CollateSequence;
import lama.sqlite3.schema.SQLite3Schema.Table.TableKind;

public class SQLite3Schema {

	private final List<Table> databaseTables;

	public static class Column implements Comparable<Column> {

		private final String name;
		private final SQLite3DataType columnType;
		private final boolean isPrimaryKey;
		private final boolean isInteger; // "INTEGER" type, not "INT"
		private Table table;
		private final CollateSequence collate;
		
		public enum CollateSequence {
			NOCASE, RTRIM, BINARY;

			public static CollateSequence random() {
				return Randomly.fromOptions(values());
			}
		}

		public Column(String name, SQLite3DataType columnType, boolean isInteger, boolean isPrimaryKey, CollateSequence collate) {
			this.name = name;
			this.columnType = columnType;
			this.isInteger = isInteger;
			this.isPrimaryKey = isPrimaryKey;
			this.collate = collate;
			assert !isInteger || columnType == SQLite3DataType.INT;
		}

		@Override
		public String toString() {
			return String.format("%s.%s: %s", table.getName(), name, columnType);
		}

		@Override
		public int hashCode() {
			return name.hashCode() + 11 * columnType.hashCode();
		}

		@Override
		public boolean equals(Object obj) {
			if (!(obj instanceof Column)) {
				return false;
			} else {
				Column c = (Column) obj;
				return table.getName().contentEquals(getName()) && name.equals(c.name);
			}
		}

		public String getName() {
			return name;
		}

		public String getFullQualifiedName() {
			return table.getName() + "." + getName();
		}

		public SQLite3DataType getColumnType() {
			return columnType;
		}

		public boolean isPrimaryKey() {
			return isPrimaryKey;
		}

		public boolean isOnlyPrimaryKey() {
			return isPrimaryKey && table.getColumns().stream().filter(c -> c.isPrimaryKey()).count() == 1;
		}

		// see https://www.sqlite.org/lang_createtable.html#rowid
		/**
		 * If a table has a single column primary key and the declared type of that
		 * column is "INTEGER" and the table is not a WITHOUT ROWID table, then the
		 * column is known as an INTEGER PRIMARY KEY.
		 */
		public boolean isIntegerPrimaryKey() {
			return isInteger && isOnlyPrimaryKey() && !table.hasWithoutRowid();
		}

		public void setTable(Table table) {
			this.table = table;
		}

		public Table getTable() {
			return table;
		}

		@Override
		public int compareTo(Column o) {
			if (o.getTable().equals(this.getTable())) {
				return name.compareTo(o.getName());
			} else {
				return o.getTable().compareTo(table);
			}
		}

		public CollateSequence getCollateSequence() {
			return collate;
		}

	}

	public static class Tables {
		private final List<Table> tables;
		private final List<Column> columns;

		public Tables(List<Table> tables) {
			this.tables = tables;
			columns = new ArrayList<>();
			for (Table t : tables) {
				columns.addAll(t.getColumns());
			}
		}

		public String tableNamesAsString() {
			return tables.stream().map(t -> t.getName()).collect(Collectors.joining(", "));
		}

		public List<Table> getTables() {
			return tables;
		}

		public List<Column> getColumns() {
			return columns;
		}

		public String columnNamesAsString() {
			return getColumns().stream().map(t -> t.getTable().getName() + "." + t.getName())
					.collect(Collectors.joining(", "));
		}

		public String columnNamesAsString(Function<Column, String> function) {
			return getColumns().stream().map(function).collect(Collectors.joining(", "));
		}

		public RowValue getRandomRowValue(Connection con, SQLite3StateToReproduce state) throws SQLException {
			String randomRow = String.format("SELECT %s, %s FROM %s ORDER BY RANDOM() LIMIT 1", columnNamesAsString(
					c -> c.getTable().getName() + "." + c.getName() + " AS " + c.getTable().getName() + c.getName()),
					columnNamesAsString(c -> "typeof(" + c.getTable().getName() + "." + c.getName() + ")"),
					tableNamesAsString());
			Map<Column, SQLite3Constant> values = new HashMap<>();
			try (Statement s = con.createStatement()) {
				ResultSet randomRowValues = s.executeQuery(randomRow);
				if (!randomRowValues.next()) {
					throw new AssertionError("could not find random row! " + randomRow + "\n" + state);
				}
				for (int i = 0; i < getColumns().size(); i++) {
					Column column = getColumns().get(i);
					Object value;
					int columnIndex = randomRowValues.findColumn(column.getTable().getName() + column.getName());
					assert columnIndex == i + 1;
					String typeString = randomRowValues.getString(columnIndex + getColumns().size());
					SQLite3DataType valueType = getColumnType(typeString);
					SQLite3Constant constant;
					if (randomRowValues.getString(columnIndex) == null) {
						value = null;
						constant = SQLite3Constant.createNullConstant();
					} else {
						switch (valueType) {
						case INT:
							value = randomRowValues.getLong(columnIndex);
							constant = SQLite3Constant.createIntConstant((long) value);
							break;
						case REAL:
							value = randomRowValues.getDouble(columnIndex);
							constant = SQLite3Constant.createRealConstant((double) value);
							break;
						case TEXT:
						case NONE:
							value = randomRowValues.getString(columnIndex);
							constant = SQLite3Constant.createTextConstant((String) value);
							break;
						case BINARY:
							value = randomRowValues.getBytes(columnIndex);
							constant = SQLite3Constant.createBinaryConstant((byte[]) value);
							break;
						default:
							throw new AssertionError(valueType);
						}
					}
					values.put(column, constant);
				}
				assert (!randomRowValues.next());
				state.randomRowValues = values;
				return new RowValue(this, values);
			}

		}
	}

	public static class Table implements Comparable<Table> {

		public static enum TableKind {
			MAIN, TEMP;
		}

		private final String tableName;
		private final List<Column> columns;
		private final TableKind tableType;
		private Column rowid;
		private boolean withoutRowid;
		private int nrRows;

		public Table(String tableName, List<Column> columns, TableKind tableType, boolean withoutRowid, int nrRows) {
			this.tableName = tableName;
			this.tableType = tableType;
			this.withoutRowid = withoutRowid;
			this.columns = Collections.unmodifiableList(columns);
			this.nrRows = nrRows;
		}

		public boolean hasWithoutRowid() {
			return withoutRowid;
		}

		@Override
		public String toString() {
			StringBuffer sb = new StringBuffer();
			sb.append(tableName + "\n");
			for (Column c : columns) {
				sb.append("\t" + c + "\n");
			}
			return sb.toString();
		}

		public String getName() {
			return tableName;
		}

		public List<Column> getColumns() {
			return columns;
		}

		public String getColumnsAsString() {
			return columns.stream().map(c -> c.getName()).collect(Collectors.joining(", "));
		}

		public String getColumnsAsString(Function<Column, String> function) {
			return columns.stream().map(function).collect(Collectors.joining(", "));
		}

		public Column getRandomColumn() {
			return Randomly.fromList(columns);
		}

		@Override
		public int compareTo(Table o) {
			return o.getName().compareTo(tableName);
		}

		public void addRowid(Column rowid) {
			this.rowid = rowid;
		}

		public Column getRowid() {
			return rowid;
		}

		public TableKind getTableType() {
			return tableType;
		}

		public List<Column> getRandomNonEmptyColumnSubset() {
			return Randomly.nonEmptySubset(getColumns());
		}

		public boolean isSystemTable() {
			return getName().startsWith("sqlit");
		}
		
		public int getNrRows() {
			return nrRows;
		}

	}

	public static class RowValue {
		private final Tables tables;
		private final Map<Column, SQLite3Constant> values;

		RowValue(Tables tables, Map<Column, SQLite3Constant> values) {
			this.tables = tables;
			this.values = values;
		}

		public Tables getTable() {
			return tables;
		}

		public Map<Column, SQLite3Constant> getValues() {
			return values;
		}

		@Override
		public String toString() {
			StringBuffer sb = new StringBuffer();
			int i = 0;
			for (Column c : tables.getColumns()) {
				if (i++ != 0) {
					sb.append(", ");
				}
				sb.append(values.get(c));
			}
			return sb.toString();
		}

		public String getRowValuesAsString() {
			List<Column> columnsToCheck = tables.getColumns();
			return getRowValuesAsString(columnsToCheck);
		}

		public String getRowValuesAsString(List<Column> columnsToCheck) {
			StringBuilder sb = new StringBuilder();
			Map<Column, SQLite3Constant> expectedValues = getValues();
			for (int i = 0; i < columnsToCheck.size(); i++) {
				if (i != 0) {
					sb.append(", ");
				}
				SQLite3Constant expectedColumnValue = expectedValues.get(columnsToCheck.get(i));
				SQLite3ToStringVisitor visitor = new SQLite3ToStringVisitor();
				visitor.visit(expectedColumnValue);
				sb.append(visitor.get());
			}
			return sb.toString();
		}

	}

	public SQLite3Schema(List<Table> databaseTables) {
		this.databaseTables = Collections.unmodifiableList(databaseTables);
	}

	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		for (Table t : getDatabaseTables()) {
			sb.append(t + "\n");
		}
		return sb.toString();
	}
	
	public static int getNrRows(Connection con, String table) throws SQLException {
		try (Statement s = con.createStatement()) {
			try (ResultSet query = s.executeQuery("SELECT COUNT(*) FROM " + table)) {
				query.next();
				return query.getInt(1);
			}
		}
	}

	static public SQLite3Schema fromConnection(Connection con) throws SQLException {
		List<Table> databaseTables = new ArrayList<>();
		try (Statement s = con.createStatement()) {
			try (ResultSet rs = s.executeQuery("SELECT name, type as category, sql FROM sqlite_master UNION "
					+ "SELECT name, 'temp_table' as category, sql FROM sqlite_temp_master WHERE type='table';")) {
				while (rs.next()) {
					String tableName = rs.getString("name");
					String tableType = rs.getString("category");
					if ((tableName.startsWith("sqlite_") /* && !tableName.startsWith("sqlite_stat") */)
							|| tableType.equals("index")) {
						continue;
					}
					String string = rs.getString("sql").toLowerCase();
					List<Column> databaseColumns = getTableColumns(con, tableName, string);
					boolean withoutRowid = string.contains("without rowid");
					Table t = new Table(tableName, databaseColumns,
							tableType.contentEquals("temp_table") ? TableKind.TEMP : TableKind.MAIN, withoutRowid, getNrRows(con, tableName));
					try (Statement s3 = con.createStatement()) {
						try (ResultSet rs3 = s3.executeQuery("SELECT typeof(rowid) FROM " + tableName)) {
							if (rs3.next()) {
								String dataType = rs3.getString(1);
								SQLite3DataType columnType = getColumnType(dataType);
								String rowId = Randomly.fromOptions("rowid", "_rowid_", "oid");
								Column rowid = new Column(rowId, columnType, true, true, null);
								t.addRowid(rowid);
								rowid.setTable(t);
							}
						}
					} catch (SQLException e) {
						// ignore
					}
					for (Column c : databaseColumns) {
						c.setTable(t);
					}
					databaseTables.add(t);
				}
			}
		}
		return new SQLite3Schema(databaseTables);
	}

	private static List<Column> getTableColumns(Connection con, String tableName, String sql) throws SQLException {
		List<Column> databaseColumns = new ArrayList<>();
		try (Statement s2 = con.createStatement()) {
			try (ResultSet columnRs = s2.executeQuery(String.format("PRAGMA table_info(%s)", tableName))) {
				String[] columnCreates = sql.split(",");
				int columnCreateIndex = 0;
				while (columnRs.next()) {
					String columnName = columnRs.getString("name");
					String columnTypeString = columnRs.getString("type");
					boolean isPrimaryKey = columnRs.getBoolean("pk");
					SQLite3DataType columnType = getColumnType(columnTypeString);
					
					sql = columnCreates[columnCreateIndex++];
					CollateSequence collate;
					if (sql.contains("collate binary")) {
						collate = CollateSequence.BINARY;
					} else if (sql.contains("collate rtrim")) {
						collate = CollateSequence.RTRIM;
					} else if (sql.contains("collate nocase")) {
						collate = CollateSequence.NOCASE;
					} else {
						collate = CollateSequence.BINARY;
					}
					databaseColumns.add(new Column(columnName, columnType, columnTypeString.contentEquals("INTEGER"),
							isPrimaryKey, collate));
				}
			}
		}
		assert !databaseColumns.isEmpty() : tableName;
		return databaseColumns;
	}

	private static SQLite3DataType getColumnType(String columnTypeString) {
		columnTypeString = columnTypeString.toUpperCase();
		SQLite3DataType columnType;
		switch (columnTypeString) {
		case "TEXT":
			columnType = SQLite3DataType.TEXT;
			break;
		case "INTEGER":
			columnType = SQLite3DataType.INT;
			break;
		case "INT":
			columnType = SQLite3DataType.INT;
			break;
		case "":
			columnType = SQLite3DataType.NONE;
			break;
		case "BLOB":
			columnType = SQLite3DataType.BINARY;
			break;
		case "REAL":
			columnType = SQLite3DataType.REAL;
			break;
		case "NULL":
			columnType = SQLite3DataType.NULL;
			break;
		default:
			throw new AssertionError(columnTypeString);
		}
		return columnType;
	}

	public Table getRandomTable() {
		return Randomly.fromList(getDatabaseTables());
	}

	public List<Table> getDatabaseTables() {
		return databaseTables;
	}

	public Tables getTables() {
		return new Tables(databaseTables);
	}

	public Tables getRandomTableNonEmptyTables() {
		return new Tables(Randomly.nonEmptySubset(databaseTables));
	}

}
