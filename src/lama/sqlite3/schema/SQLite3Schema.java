package lama.sqlite3.schema;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import lama.IgnoreMeException;
import lama.QueryAdapter;
import lama.Randomly;
import lama.StateToReproduce.SQLite3StateToReproduce;
import lama.sqlite3.SQLite3Errors;
import lama.sqlite3.SQLite3ToStringVisitor;
import lama.sqlite3.ast.SQLite3Constant;
import lama.sqlite3.schema.SQLite3Schema.SQLite3Column.CollateSequence;
import lama.sqlite3.schema.SQLite3Schema.Table.TableKind;

public class SQLite3Schema {

	private final List<Table> databaseTables;
	private final List<String> indexNames;

	public List<String> getIndexNames() {
		return indexNames;
	}

	public String getRandomIndexOrBailout() {
		if (indexNames.size() == 0) {
			throw new IgnoreMeException();
		} else {
			return Randomly.fromList(indexNames);
		}
	}

	public Table getRandomTableOrBailout() {
		if (databaseTables.isEmpty()) {
			throw new IgnoreMeException();
		} else {
			return Randomly.fromList(getDatabaseTables());
		}
	}

	public static class SQLite3Column implements Comparable<SQLite3Column> {

		private final String name;
		private final SQLite3DataType columnType;
		private final boolean isPrimaryKey;
		private final boolean isInteger; // "INTEGER" type, not "INT"
		private Table table;
		private final CollateSequence collate;
		private boolean generated;

		public enum CollateSequence {
			NOCASE, RTRIM, BINARY;

			public static CollateSequence random() {
				return Randomly.fromOptions(values());
			}
		}

		public SQLite3Column(String name, SQLite3DataType columnType, boolean isInteger, boolean isPrimaryKey,
				CollateSequence collate) {
			this.name = name;
			this.columnType = columnType;
			this.isInteger = isInteger;
			this.isPrimaryKey = isPrimaryKey;
			this.collate = collate;
			this.generated = false;
			assert !isInteger || columnType == SQLite3DataType.INT;
		}

		public SQLite3Column(String rowId, SQLite3DataType columnType2, boolean contains, boolean b, CollateSequence collate,
				boolean generated) {
			this(rowId, columnType2, b, generated, collate);
			this.generated = generated;
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
			if (!(obj instanceof SQLite3Column)) {
				return false;
			} else {
				SQLite3Column c = (SQLite3Column) obj;
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
		public int compareTo(SQLite3Column o) {
			if (o.getTable().equals(this.getTable())) {
				return name.compareTo(o.getName());
			} else {
				return o.getTable().compareTo(table);
			}
		}

		public CollateSequence getCollateSequence() {
			return collate;
		}
		
		public boolean isGenerated() {
			return generated;
		}

		public static SQLite3Column createDummy(String name) {
			return new SQLite3Column(name, SQLite3DataType.INT, false, false, null);
		}

	}

	public static class Tables {
		private final List<Table> tables;
		private final List<SQLite3Column> columns;

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

		public List<SQLite3Column> getColumns() {
			return columns;
		}

		public String columnNamesAsString() {
			return getColumns().stream().map(t -> t.getTable().getName() + "." + t.getName())
					.collect(Collectors.joining(", "));
		}

		public String columnNamesAsString(Function<SQLite3Column, String> function) {
			return getColumns().stream().map(function).collect(Collectors.joining(", "));
		}

		public RowValue getRandomRowValue(Connection con, SQLite3StateToReproduce state) throws SQLException {
			String randomRow = String.format("SELECT %s, %s FROM %s ORDER BY RANDOM() LIMIT 1", columnNamesAsString(
					c -> c.getTable().getName() + "." + c.getName() + " AS " + c.getTable().getName() + c.getName()),
					columnNamesAsString(c -> "typeof(" + c.getTable().getName() + "." + c.getName() + ")"),
					tableNamesAsString());
			Map<SQLite3Column, SQLite3Constant> values = new HashMap<>();
			try (Statement s = con.createStatement()) {
				ResultSet randomRowValues;
				try {
					randomRowValues = s.executeQuery(randomRow);
				} catch (SQLException e) {
					throw new IgnoreMeException();
				}
				if (!randomRowValues.next()) {
					throw new AssertionError("could not find random row! " + randomRow + "\n" + state);
				}
				for (int i = 0; i < getColumns().size(); i++) {
					SQLite3Column column = getColumns().get(i);
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
		private final List<SQLite3Column> columns;
		private final TableKind tableType;
		private SQLite3Column rowid;
		private boolean withoutRowid;
		private int nrRows;
		private boolean isView;
		private boolean isVirtual;

		public Table(String tableName, List<SQLite3Column> columns, TableKind tableType, boolean withoutRowid, int nrRows,
				boolean isView, boolean isVirtual) {
			this.tableName = tableName;
			this.tableType = tableType;
			this.withoutRowid = withoutRowid;
			this.isView = isView;
			this.isVirtual = isVirtual;
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
			for (SQLite3Column c : columns) {
				sb.append("\t" + c + "\n");
			}
			return sb.toString();
		}

		public String getName() {
			return tableName;
		}

		public List<SQLite3Column> getColumns() {
			return columns;
		}

		public String getColumnsAsString() {
			return columns.stream().map(c -> c.getName()).collect(Collectors.joining(", "));
		}

		public String getColumnsAsString(Function<SQLite3Column, String> function) {
			return columns.stream().map(function).collect(Collectors.joining(", "));
		}

		public SQLite3Column getRandomColumn() {
			return Randomly.fromList(columns);
		}

		@Override
		public int compareTo(Table o) {
			return o.getName().compareTo(tableName);
		}

		public void addRowid(SQLite3Column rowid) {
			this.rowid = rowid;
		}

		public SQLite3Column getRowid() {
			return rowid;
		}

		public TableKind getTableType() {
			return tableType;
		}

		public boolean isVirtual() {
			return isVirtual;
		}

		public List<SQLite3Column> getRandomNonEmptyColumnSubset() {
			return Randomly.nonEmptySubset(getColumns());
		}

		public boolean isSystemTable() {
			return getName().startsWith("sqlit");
		}

		public int getNrRows() {
			return nrRows;
		}

		public boolean isView() {
			return isView;
		}

		public boolean isTemp() {
			return tableType == TableKind.TEMP;
		}

	}

	public static class RowValue {
		private final Tables tables;
		private final Map<SQLite3Column, SQLite3Constant> values;

		RowValue(Tables tables, Map<SQLite3Column, SQLite3Constant> values) {
			this.tables = tables;
			this.values = values;
		}

		public Tables getTable() {
			return tables;
		}

		public Map<SQLite3Column, SQLite3Constant> getValues() {
			return values;
		}

		@Override
		public String toString() {
			StringBuffer sb = new StringBuffer();
			int i = 0;
			for (SQLite3Column c : tables.getColumns()) {
				if (i++ != 0) {
					sb.append(", ");
				}
				sb.append(values.get(c));
			}
			return sb.toString();
		}

		public String getRowValuesAsString() {
			List<SQLite3Column> columnsToCheck = tables.getColumns();
			return getRowValuesAsString(columnsToCheck);
		}

		public String getRowValuesAsString(List<SQLite3Column> columnsToCheck) {
			StringBuilder sb = new StringBuilder();
			Map<SQLite3Column, SQLite3Constant> expectedValues = getValues();
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

	public SQLite3Schema(List<Table> databaseTables, List<String> indexNames) {
		this.indexNames = indexNames;
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
		String string = "SELECT COUNT(*) FROM " + table;
		List<String> errors = new ArrayList<>();
		errors.add("ORDER BY term out of range");
		errors.addAll(Arrays.asList("second argument to nth_value must be a positive integer",
				"ON clause references tables to its right", "no such table", "no query solution", "no such index",
				"GROUP BY term", "is circularly defined", "misuse of aggregate", "no such column", "misuse of window function"));
		SQLite3Errors.addExpectedExpressionErrors(errors);
		QueryAdapter q = new QueryAdapter(string, errors);
		try (ResultSet query = q.executeAndGet(con)) {
			if (query == null) {
				throw new IgnoreMeException();
			}
			query.next();
			int int1 = query.getInt(1);
			query.getStatement().close();
			return int1;
		}
	}

	static public SQLite3Schema fromConnection(Connection con) throws SQLException {
		List<Table> databaseTables = new ArrayList<>();
		List<String> indexNames = new ArrayList<>();

		try (Statement s = con.createStatement()) {
			try (ResultSet rs = s.executeQuery("SELECT name, type as category, sql FROM sqlite_master UNION "
					+ "SELECT name, 'temp_table' as category, sql FROM sqlite_temp_master WHERE type='table' UNION SELECT name, 'view' as category, sql FROM sqlite_temp_master WHERE type='view';")) {
				while (rs.next()) {
					String tableName = rs.getString("name");
					String tableType = rs.getString("category");
					if ((tableName.startsWith("sqlite_") /* && !tableName.startsWith("sqlite_stat") */)
							|| tableType.equals("index") || tableType.equals("trigger") || tableName.endsWith("_idx")
							|| tableName.endsWith("_docsize") || tableName.endsWith("_content")
							|| tableName.endsWith("_data") || tableName.endsWith("_config")
							|| tableName.endsWith("_segdir") || tableName.endsWith("_stat")
							|| tableName.endsWith("_segments") || tableName.contains("_")) {
						continue;
					}
					String sqlString = rs.getString("sql").toLowerCase();
					boolean withoutRowid = sqlString.contains("without rowid");
					boolean isView = tableType.contentEquals("view");
					boolean isVirtual = sqlString.contains("virtual");
					List<SQLite3Column> databaseColumns;
					try {
						databaseColumns = getTableColumns(con, tableName, sqlString, isView);
					} catch (IgnoreMeException e) {
						continue;
					}
					int nrRows;
					try {
						nrRows = getNrRows(con, tableName);
					} catch (IgnoreMeException e) {
						nrRows = 0;
					}
					Table t = new Table(tableName, databaseColumns,
							tableType.contentEquals("temp_table") ? TableKind.TEMP : TableKind.MAIN, withoutRowid,
							nrRows, isView, isVirtual);
					try (Statement s3 = con.createStatement()) {
						try (ResultSet rs3 = s3.executeQuery("SELECT typeof(rowid) FROM " + tableName)) {
							if (rs3.next() && !isView /* TODO: can we still do something with it? */) {
								String dataType = rs3.getString(1);
								SQLite3DataType columnType = getColumnType(dataType);
								boolean generated = dataType.toUpperCase().contains("GENERATED AS");
								String rowId = Randomly.fromOptions("rowid", "_rowid_", "oid");
								SQLite3Column rowid = new SQLite3Column(rowId, columnType, dataType.contains("INTEGER"), true, null, generated);
								t.addRowid(rowid);
								rowid.setTable(t);
							}
						}
					} catch (SQLException e) {
						// ignore
					}
					for (SQLite3Column c : databaseColumns) {
						c.setTable(t);
					}
					databaseTables.add(t);
				}
			}
			try (ResultSet rs = s.executeQuery(
					"SELECT name FROM SQLite_master WHERE type = 'index' UNION SELECT name FROM sqlite_temp_master WHERE type='index'")) {
				while (rs.next()) {
					String name = rs.getString(1);
					if (name.contains("_autoindex")) {
						continue;
					}
					indexNames.add(name);
				}
			}
		}

		return new SQLite3Schema(databaseTables, indexNames);
	}

	private static List<SQLite3Column> getTableColumns(Connection con, String tableName, String sql, boolean isView)
			throws SQLException {
		List<SQLite3Column> databaseColumns = new ArrayList<>();
		try (Statement s2 = con.createStatement()) {
			String tableInfoStr = String.format("PRAGMA table_xinfo(%s)", tableName);
			try (ResultSet columnRs = s2.executeQuery(tableInfoStr)) {
				String[] columnCreates = sql.split(",");
				int columnCreateIndex = 0;
				while (columnRs.next()) {
					String columnName = columnRs.getString("name");
					if (columnName.contentEquals("docid") || columnName.contentEquals("rank") || columnName.contentEquals(tableName) || columnName.contentEquals("__langid")) {
						continue; // internal column names of FTS tables
					}
					String columnTypeString = columnRs.getString("type");
					boolean isPrimaryKey = columnRs.getBoolean("pk");
					SQLite3DataType columnType = getColumnType(columnTypeString);
					sql = columnCreates[columnCreateIndex++];
					CollateSequence collate;
					if (isView) {
						collate = CollateSequence.BINARY;
					} else {
						if (sql.contains("collate binary")) {
							collate = CollateSequence.BINARY;
						} else if (sql.contains("collate rtrim")) {
							collate = CollateSequence.RTRIM;
						} else if (sql.contains("collate nocase")) {
							collate = CollateSequence.NOCASE;
						} else {
							collate = CollateSequence.BINARY;
						}
					}
					databaseColumns.add(new SQLite3Column(columnName, columnType, columnTypeString.contentEquals("INTEGER"),
							isPrimaryKey, collate));
				}
			} catch (SQLException | ArrayIndexOutOfBoundsException e) {
				throw new IgnoreMeException(); // TODO
				// throw new AssertionError(tableInfoStr);
			}
		}
		if (databaseColumns.isEmpty()) {
			// only generated columns
			throw new IgnoreMeException();
		}
		assert !databaseColumns.isEmpty() : tableName;
		return databaseColumns;
	}

	private static SQLite3DataType getColumnType(String columnTypeString) {
		columnTypeString = columnTypeString.toUpperCase().replace(" GENERATED ALWAYS", "");
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
		case "NUM":
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

	public Table getRandomTable(Predicate<Table> predicate) {
		return Randomly.fromList(databaseTables.stream().filter(predicate).collect(Collectors.toList()));
	}

	public List<Table> getTables(Predicate<Table> predicate) {
		return databaseTables.stream().filter(predicate).collect(Collectors.toList());
	}

	public Table getRandomTableOrBailout(Predicate<Table> predicate) {
		List<Table> tables = databaseTables.stream().filter(predicate).collect(Collectors.toList());
		if (tables.isEmpty()) {
			throw new IgnoreMeException();
		} else {
			return Randomly.fromList(tables);
		}
	}

	public Table getRandomVirtualTable() {
		return getRandomTable(p -> p.isVirtual);
	}

	public List<Table> getDatabaseTables() {
		return databaseTables;
	}

	public Tables getTables() {
		return new Tables(databaseTables);
	}

	public Tables getRandomTableNonEmptyTables() {
		if (databaseTables.isEmpty()) {
			throw new IgnoreMeException();
		}
		return new Tables(Randomly.nonEmptySubset(databaseTables));
	}

	public Table getRandomTableNoViewOrBailout() {
		List<Table> databaseTablesWithoutViews = getDatabaseTablesWithoutViews();
		if (databaseTablesWithoutViews.isEmpty()) {
			throw new IgnoreMeException();
		}
		return Randomly.fromList(databaseTablesWithoutViews);
	}

	public Table getRandomTableNoViewNoVirtualTable() {
		return Randomly.fromList(getDatabaseTablesWithoutViewsWithoutVirtualTables());
	}

	public List<Table> getDatabaseTablesWithoutViews() {
		return databaseTables.stream().filter(t -> !t.isView).collect(Collectors.toList());
	}

	public List<Table> getViews() {
		return databaseTables.stream().filter(t -> t.isView).collect(Collectors.toList());
	}

	public Table getRandomViewOrBailout() {
		if (getViews().isEmpty()) {
			throw new IgnoreMeException();
		} else {
			return Randomly.fromList(getViews());
		}
	}

	public List<Table> getDatabaseTablesWithoutViewsWithoutVirtualTables() {
		return databaseTables.stream().filter(t -> !t.isView && !t.isVirtual).collect(Collectors.toList());
	}

}
