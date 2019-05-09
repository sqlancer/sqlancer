package lama.schema;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
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

import lama.Expression.Constant;
import lama.Main.StateToReproduce;
import lama.Randomly;
import lama.sqlite3.SQLite3Visitor;

public class Schema {

	private final List<Table> databaseTables;

	public static class Column implements Comparable<Column> {

		private final String name;
		private final SQLite3DataType columnType;
		private final boolean isPrimaryKey;
		private final boolean isInteger; // "INTEGER" type, not "INT"
		private Table table;

		Column(String name, SQLite3DataType columnType, boolean isInteger, boolean isPrimaryKey) {
			this.name = name;
			this.columnType = columnType;
			this.isInteger = isInteger;
			this.isPrimaryKey = isPrimaryKey;
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

		public RowValue getRandomRowValue(Connection con, StateToReproduce state) throws SQLException {
			String randomRow = String.format("SELECT %s, %s FROM %s ORDER BY RANDOM() LIMIT 1", columnNamesAsString(
					c -> c.getTable().getName() + "." + c.getName() + " AS " + c.getTable().getName() + c.getName()),
					columnNamesAsString(c -> "typeof(" + c.getTable().getName() + "." + c.getName() + ")"),
					tableNamesAsString());
			Map<Column, Constant> values = new HashMap<>();
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
					if (randomRowValues.getString(columnIndex) == null) {
						value = null;
					} else {
						switch (valueType) {
						case INT:
							value = randomRowValues.getLong(columnIndex);
							break;
						case REAL:
							value = randomRowValues.getDouble(columnIndex);
							break;
						case TEXT:
						case NONE:
							value = randomRowValues.getString(columnIndex);
							break;
						case BINARY:
							value = randomRowValues.getBytes(columnIndex);
							break;
						default:
							throw new AssertionError();
						}
					}
					values.put(column, Constant.create(value, valueType));
				}
				assert (!randomRowValues.next());
				state.randomRowValues = values;
				return new RowValue(this, values);
			}

		}
	}

	public static class Table implements Comparable<Table> {

		private final String tableName;
		private final List<Column> columns;
		private Column rowid;

		public Table(String tableName, List<Column> columns) {
			this.tableName = tableName;
			this.columns = Collections.unmodifiableList(columns);
		}

		public boolean hasWithoutRowid() {
			// TODO FIXME implement
			return false;
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

	}

	public static class RowValue {
		private final Tables tables;
		private final Map<Column, Constant> values;

		RowValue(Tables tables, Map<Column, Constant> values) {
			this.tables = tables;
			this.values = values;
		}

		public Tables getTable() {
			return tables;
		}

		public Map<Column, Constant> getValues() {
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
			Map<Column, Constant> expectedValues = getValues();
			for (int i = 0; i < columnsToCheck.size(); i++) {
				if (i != 0) {
					sb.append(", ");
				}
				Constant expectedColumnValue = expectedValues.get(columnsToCheck.get(i));
				SQLite3Visitor visitor = new SQLite3Visitor();
				visitor.visit(expectedColumnValue);
				sb.append(visitor.get());
			}
			return sb.toString();
		}

	}

	public Schema(List<Table> databaseTables) {
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

	static public Schema fromConnection(Connection con) throws SQLException {
		DatabaseMetaData meta = con.getMetaData();
		List<Table> databaseTables = new ArrayList<>();
		try (ResultSet tables = meta.getTables(null, null, null, null);) {
			while (tables.next()) {
				String tableName = tables.getString(3);
				if (tables.getString(4).equals("SYSTEM TABLE") || tables.getString(4).equals("SYSTEM VIEW")
						|| tables.getString(4).equals("VIEW")) {
					continue;
				}
				try (ResultSet columns = meta.getColumns(null, null, tableName, null)) {
					List<Column> databaseColumns = new ArrayList<>();
					List<String> primaryKeysMap = new ArrayList<>();
					try (ResultSet primaryKeys = meta.getPrimaryKeys(null, null, tableName)) {
						while (primaryKeys.next()) {
							primaryKeysMap.add(primaryKeys.getString("COLUMN_NAME"));
						}
						while (columns.next()) {
							String columnName = columns.getString(4);
							String columnTypeString = columns.getString(6);
							SQLite3DataType columnType = getColumnType(columnTypeString);
							databaseColumns.add(new Column(columnName, columnType,
									columnTypeString.contentEquals("INTEGER"), primaryKeysMap.contains(columnName)));
						}

						Table t = new Table(tableName, databaseColumns);
						try (Statement s = con.createStatement()) {
							try (ResultSet rs = s.executeQuery("SELECT typeof(rowid) FROM " + tableName)) {
								if (rs.next()) {
									String dataType = rs.getString(1);
									SQLite3DataType columnType = getColumnType(dataType);
									Column rowid = new Column("rowid", columnType, true, true);
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
			tables.getStatement().close();
		}
		return new Schema(databaseTables);
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

	public Tables getRandomTableNonEmptyTables() {
		return new Tables(Randomly.nonEmptySubset(databaseTables));
	}

}
