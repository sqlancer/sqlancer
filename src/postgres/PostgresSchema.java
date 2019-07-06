package postgres;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import lama.Randomly;
import lama.StateToReproduce.PostgresStateToReproduce;
import lama.sqlite3.schema.SQLite3Schema.Column;
import postgres.PostgresSchema.PostgresTable.TableType;
import postgres.ast.PostgresConstant;

public class PostgresSchema {

	public static enum PostgresDataType {
		INT, BOOLEAN, TEXT;

		public static PostgresDataType getRandomType() {
			return Randomly.fromOptions(values());
		}
	}

	public static class PostgresColumn implements Comparable<PostgresColumn> {

		private final String name;
		private final PostgresDataType columnType;
		private PostgresTable table;
		private int precision;

		public enum CollateSequence {
			NOCASE, RTRIM, BINARY;

			public static CollateSequence random() {
				return Randomly.fromOptions(values());
			}
		}

		public PostgresColumn(String name, PostgresDataType columnType) {
			this.name = name;
			this.columnType = columnType;
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
				PostgresColumn c = (PostgresColumn) obj;
				return table.getName().contentEquals(getName()) && name.equals(c.name);
			}
		}

		public String getName() {
			return name;
		}

		public String getFullQualifiedName() {
			return table.getName() + "." + getName();
		}

		public PostgresDataType getColumnType() {
			return columnType;
		}

		public int getPrecision() {
			return precision;
		}

		public void setTable(PostgresTable t) {
			this.table = t;
		}

		public PostgresTable getTable() {
			return table;
		}

		@Override
		public int compareTo(PostgresColumn o) {
			if (o.getTable().equals(this.getTable())) {
				return name.compareTo(o.getName());
			} else {
				return o.getTable().compareTo(table);
			}
		}

	}

	public static class PostgresTables {
		private final List<PostgresTable> tables;
		private final List<PostgresColumn> columns;

		public PostgresTables(List<PostgresTable> tables) {
			this.tables = tables;
			columns = new ArrayList<>();
			for (PostgresTable t : tables) {
				columns.addAll(t.getColumns());
			}
		}

		public String tableNamesAsString() {
			return tables.stream().map(t -> t.getName()).collect(Collectors.joining(", "));
		}

		public List<PostgresTable> getTables() {
			return tables;
		}

		public List<PostgresColumn> getColumns() {
			return columns;
		}

		public String columnNamesAsString() {
			return getColumns().stream().map(t -> t.getTable().getName() + "." + t.getName())
					.collect(Collectors.joining(", "));
		}

		public String columnNamesAsString(Function<PostgresColumn, String> function) {
			return getColumns().stream().map(function).collect(Collectors.joining(", "));
		}

		public PostgresRowValue getRandomRowValue(Connection con, PostgresStateToReproduce state) throws SQLException {
			String randomRow = String.format("SELECT %s FROM %s ORDER BY RANDOM() LIMIT 1", columnNamesAsString(
					c -> c.getTable().getName() + "." + c.getName() + " AS " + c.getTable().getName() + c.getName()),
					// columnNamesAsString(c -> "typeof(" + c.getTable().getName() + "." +
					// c.getName() + ")")
					tableNamesAsString());
			Map<PostgresColumn, PostgresConstant> values = new HashMap<>();
			try (Statement s = con.createStatement()) {
				ResultSet randomRowValues = s.executeQuery(randomRow);
				if (!randomRowValues.next()) {
					throw new AssertionError("could not find random row! " + randomRow + "\n" + state);
				}
				for (int i = 0; i < getColumns().size(); i++) {
					PostgresColumn column = getColumns().get(i);
					int columnIndex = randomRowValues.findColumn(column.getTable().getName() + column.getName());
					assert columnIndex == i + 1;
					PostgresConstant constant;
					if (randomRowValues.getString(columnIndex) == null) {
						constant = PostgresConstant.createNullConstant();
					} else {
						switch (column.getColumnType()) {
						case INT:
							constant = PostgresConstant.createIntConstant(randomRowValues.getLong(columnIndex));
							break;
						case BOOLEAN:
							constant = PostgresConstant.createBooleanConstant(randomRowValues.getBoolean(columnIndex));
							break;
						case TEXT:
							constant = PostgresConstant.createTextConstant(randomRowValues.getString(columnIndex));
							break;
						default:
							throw new AssertionError(column.getColumnType());
						}
					}
					values.put(column, constant);
				}
				assert (!randomRowValues.next());
				state.randomRowValues = values;
				return new PostgresRowValue(this, values);
			}

		}

	}

	private static PostgresDataType getColumnType(String typeString) {
		switch (typeString) {
		case "smallint":
		case "integer":
		case "bigint":
			return PostgresDataType.INT;
		case "boolean":
			return PostgresDataType.BOOLEAN;
		case "text":
			return PostgresDataType.TEXT;
		default:
			throw new AssertionError(typeString);
		}
	}

	public static class PostgresRowValue {

		private final PostgresTables tables;
		private final Map<PostgresColumn, PostgresConstant> values;

		PostgresRowValue(PostgresTables tables, Map<PostgresColumn, PostgresConstant> values) {
			this.tables = tables;
			this.values = values;
		}

		public PostgresTables getTable() {
			return tables;
		}

		public Map<PostgresColumn, PostgresConstant> getValues() {
			return values;
		}

		@Override
		public String toString() {
			StringBuffer sb = new StringBuffer();
			int i = 0;
			for (PostgresColumn c : tables.getColumns()) {
				if (i++ != 0) {
					sb.append(", ");
				}
				sb.append(values.get(c));
			}
			return sb.toString();
		}

		public String getRowValuesAsString() {
			List<PostgresColumn> columnsToCheck = tables.getColumns();
			return getRowValuesAsString(columnsToCheck);
		}

		public String getRowValuesAsString(List<PostgresColumn> columnsToCheck) {
			StringBuilder sb = new StringBuilder();
			Map<PostgresColumn, PostgresConstant> expectedValues = getValues();
			for (int i = 0; i < columnsToCheck.size(); i++) {
				if (i != 0) {
					sb.append(", ");
				}
				PostgresConstant expectedColumnValue = expectedValues.get(columnsToCheck.get(i));
				PostgresToStringVisitor visitor = new PostgresToStringVisitor();
				visitor.visit(expectedColumnValue);
				sb.append(visitor.get());
			}
			return sb.toString();
		}

	}

	public static class PostgresTable implements Comparable<PostgresTable> {
		
		public enum TableType {
			STANDARD, TEMPORARY
		}

		private final String tableName;
		private final List<PostgresColumn> columns;
		private final List<PostgresIndex> indexes;
		private final TableType tableType;

		public PostgresTable(String tableName, List<PostgresColumn> columns, List<PostgresIndex> indexes, TableType tableType) {
			this.tableName = tableName;
			this.indexes = indexes;
			this.columns = Collections.unmodifiableList(columns);
			this.tableType = tableType;
		}

		@Override
		public String toString() {
			StringBuffer sb = new StringBuffer();
			sb.append(tableName + "\n");
			for (PostgresColumn c : columns) {
				sb.append("\t" + c + "\n");
			}
			return sb.toString();
		}

		public List<PostgresIndex> getIndexes() {
			return indexes;
		}

		public String getName() {
			return tableName;
		}

		public List<PostgresColumn> getColumns() {
			return columns;
		}

		public String getColumnsAsString() {
			return columns.stream().map(c -> c.getName()).collect(Collectors.joining(", "));
		}

		public String getColumnsAsString(Function<PostgresColumn, String> function) {
			return columns.stream().map(function).collect(Collectors.joining(", "));
		}

		public PostgresColumn getRandomColumn() {
			return Randomly.fromList(columns);
		}

		public boolean hasIndexes() {
			return !indexes.isEmpty();
		}

		public PostgresIndex getRandomIndex() {
			return Randomly.fromList(indexes);
		}

		@Override
		public int compareTo(PostgresTable o) {
			return o.getName().compareTo(tableName);
		}

		public List<PostgresColumn> getRandomNonEmptyColumnSubset() {
			return Randomly.nonEmptySubset(getColumns());
		}
		

		public List<PostgresColumn> getRandomNonEmptyColumnSubset(int size) {
			while (true) {
				// FIXME
				List<PostgresColumn> cols = getRandomNonEmptyColumnSubset();
				if (cols.size() == size) {
					return cols;
				}
			}
		}
		
		public TableType getTableType() {
			return tableType;
		}

	}

	public static final class PostgresIndex {

		private final String indexName;

		private PostgresIndex(String indexName) {
			this.indexName = indexName;
		}

		public static PostgresIndex create(String indexName) {
			return new PostgresIndex(indexName);
		}

		public String getIndexName() {
			if (indexName.contentEquals("PRIMARY")) {
				return "`PRIMARY`";
			} else {
				return indexName;
			}
		}

	}

	static public PostgresSchema fromConnection(Connection con, String databaseName) throws SQLException {
		Exception ex = null;
		try {
			List<PostgresTable> databaseTables = new ArrayList<>();
			try (Statement s = con.createStatement()) {
				try (ResultSet rs = s.executeQuery(
						"SELECT table_name, table_schema FROM information_schema.tables WHERE table_schema='public' OR table_schema LIKE 'pg_temp_%';")) {
					while (rs.next()) {
						String tableName = rs.getString("table_name");
						String tableTypeStr = rs.getString("table_schema");
						PostgresTable.TableType tableType = getTableType(tableTypeStr);
						List<PostgresColumn> databaseColumns = getTableColumns(con, tableName, databaseName);
						List<PostgresIndex> indexes = getIndexes(con, tableName, databaseName);
						PostgresTable t = new PostgresTable(tableName, databaseColumns, indexes, tableType);
						for (PostgresColumn c : databaseColumns) {
							c.setTable(t);
						}
						databaseTables.add(t);
					}
				}
			}
			return new PostgresSchema(databaseTables, databaseName);
		} catch (SQLIntegrityConstraintViolationException e) {
			ex = e;
		}
		throw new AssertionError(ex);
	}

	private static PostgresTable.TableType getTableType(String tableTypeStr) throws AssertionError {
		PostgresTable.TableType tableType;
		if (tableTypeStr.contentEquals("public")) {
			tableType = TableType.STANDARD;
		} else if (tableTypeStr.startsWith("pg_temp")) {
			tableType = TableType.TEMPORARY;
		} else {
			throw new AssertionError(tableTypeStr);
		}
		return tableType;
	}

	private static List<PostgresIndex> getIndexes(Connection con, String tableName, String databaseName)
			throws SQLException {
		List<PostgresIndex> indexes = new ArrayList<>();
		try (Statement s = con.createStatement()) {
			try (ResultSet rs = s.executeQuery(
					String.format("SELECT indexname FROM pg_indexes WHERE tablename='%s';", tableName))) {
				while (rs.next()) {
					String indexName = rs.getString("indexname");
					indexes.add(PostgresIndex.create(indexName));
				}
			}
		}
		return indexes;
	}

	private static List<PostgresColumn> getTableColumns(Connection con, String tableName, String databaseName)
			throws SQLException {
		List<PostgresColumn> columns = new ArrayList<>();
		try (Statement s = con.createStatement()) {
			try (ResultSet rs = s
					.executeQuery("select column_name, data_type from INFORMATION_SCHEMA.COLUMNS where table_name = '"
							+ tableName + "'")) {
				while (rs.next()) {
					String columnName = rs.getString("column_name");
					String dataType = rs.getString("data_type");
					PostgresColumn c = new PostgresColumn(columnName, getColumnType(dataType));
					columns.add(c);
				}
			}
		}
		return columns;
	}

	private final List<PostgresTable> databaseTables;
	private String databaseName;

	public PostgresSchema(List<PostgresTable> databaseTables, String databaseName) {
		this.databaseTables = Collections.unmodifiableList(databaseTables);
		this.databaseName = databaseName;
	}

	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		for (PostgresTable t : getDatabaseTables()) {
			sb.append(t + "\n");
		}
		return sb.toString();
	}

	public PostgresTable getRandomTable() {
		return Randomly.fromList(getDatabaseTables());
	}

	public PostgresTables getRandomTableNonEmptyTables() {
		return new PostgresTables(Randomly.nonEmptySubset(databaseTables));
	}

	public List<PostgresTable> getDatabaseTables() {
		return databaseTables;
	}

	public List<PostgresTable> getDatabaseTablesRandomSubsetNotEmpty() {
		return Randomly.nonEmptySubset(databaseTables);
	}

	public String getDatabaseName() {
		return databaseName;
	}

}
