package sqlancer.tdengine;

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

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.StateToReproduce.TDEngineStateToReproduce;
import sqlancer.sqlite3.schema.SQLite3Schema.SQLite3Column;
import sqlancer.tdengine.expr.TDEngineConstant;

public class TDEngineSchema {

	public static enum TDEngineDataType {
		TIMESTAMP, INT, BOOL, FLOAT, DOUBLE, TEXT;

		public static TDEngineDataType getRandomType() {
			return Randomly.fromOptions(values());
		}
	}

	public static class TDEngineColumn implements Comparable<TDEngineColumn> {

		private final String name;
		private final TDEngineDataType columnType;
		private TDEngineTable table;
		private int precision;
		private int length;

		public TDEngineColumn(String name, TDEngineDataType columnType, int length) {
			this.name = name;
			this.columnType = columnType;
			this.length = length;
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
				TDEngineColumn c = (TDEngineColumn) obj;
				return table.getName().contentEquals(getName()) && name.equals(c.name);
			}
		}

		public String getName() {
			return name;
		}

		public int getLength() {
			return length;
		}

		public String getFullQualifiedName() {
			if (table == null) {
				return getName();
			} else {
				return table.getName() + "." + getName();
			}
		}

		public TDEngineDataType getColumnType() {
			return columnType;
		}

		public int getPrecision() {
			return precision;
		}

		public void setTable(TDEngineTable t) {
			this.table = t;
		}

		public TDEngineTable getTable() {
			return table;
		}

		@Override
		public int compareTo(TDEngineColumn o) {
			if (o.getTable().equals(this.getTable())) {
				return name.compareTo(o.getName());
			} else {
				return o.getTable().compareTo(table);
			}
		}

	}

	public static class TDEngineTables {
		private final List<TDEngineTable> tables;
		private final List<TDEngineColumn> columns;

		public TDEngineTables(List<TDEngineTable> tables) {
			this.tables = tables;
			columns = new ArrayList<>();
			for (TDEngineTable t : tables) {
				columns.addAll(t.getColumns());
			}
		}

		public String tableNamesAsString() {
			return tables.stream().map(t -> t.getName()).collect(Collectors.joining(", "));
		}

		public List<TDEngineTable> getTables() {
			return tables;
		}

		public List<TDEngineColumn> getColumns() {
			return columns;
		}

		public String columnNamesAsString() {
			return getColumns().stream().map(t -> t.getTable().getName() + "." + t.getName())
					.collect(Collectors.joining(", "));
		}

		public String columnNamesAsString(Function<TDEngineColumn, String> function) {
			return getColumns().stream().map(function).collect(Collectors.joining(", "));
		}

		public TDEngineRowValue getRandomRowValue(Connection con, TDEngineStateToReproduce state) throws SQLException {
			String randomRow = String.format("SELECT %s FROM %s LIMIT 1", columnNamesAsString( // TODO ORDER BY RANDOM() 
					c -> c.getTable().getName() + "." + c.getName() + " AS " + c.getTable().getName() + c.getName()),
					// columnNamesAsString(c -> "typeof(" + c.getTable().getName() + "." +
					// c.getName() + ")")
					tableNamesAsString());
			Map<TDEngineColumn, TDEngineConstant> values = new HashMap<>();
			try (Statement s = con.createStatement()) {
				ResultSet randomRowValues = s.executeQuery(randomRow);
				if (!randomRowValues.next()) {
					throw new IgnoreMeException();
				}
				for (int i = 0; i < getColumns().size(); i++) {
					TDEngineColumn column = getColumns().get(i);
					int columnIndex = randomRowValues.findColumn(column.getTable().getName() + column.getName());
					assert columnIndex == i + 1;
					TDEngineConstant constant = null;
					if (randomRowValues.getString(columnIndex) == null) {
						constant = TDEngineConstant.createNull();
					} else {
						switch (column.getColumnType()) {
						case INT:
							constant = TDEngineConstant.createIntConstant(randomRowValues.getLong(columnIndex));
							break;
						case BOOL:
							constant = TDEngineConstant.createBoolConstant(randomRowValues.getBoolean(columnIndex));
							break;
						case TEXT:
							constant = TDEngineConstant.createTextConstant(randomRowValues.getString(columnIndex));
							break;
						case DOUBLE:
						case FLOAT:
							constant = TDEngineConstant.createDoubleConstant(randomRowValues.getDouble(columnIndex));
							break;
						case TIMESTAMP:
							constant = TDEngineConstant.createTimestampConstant(randomRowValues.getLong(columnIndex));
							break;
						default:
							throw new AssertionError(column.getColumnType());
						}
					}
					values.put(column, constant);
				}
				assert (!randomRowValues.next());
				state.randomRowValues = values;
				return new TDEngineRowValue(this, values);
			}

		}

	}

	private static TDEngineDataType getColumnType(String typeString) {
		switch (typeString) {
		case "TIMESTAMP":
			return TDEngineDataType.TIMESTAMP;
		case "TINYINT":
		case "SMALLINT":
		case "INT":
		case "BIGINT":
			return TDEngineDataType.INT;
		case "BOOL":
			return TDEngineDataType.BOOL;
		case "FLOAT":
			return TDEngineDataType.FLOAT;
		case "DOUBLE":
			return TDEngineDataType.DOUBLE;
		default:
			if (typeString.startsWith("BINARY") || typeString.startsWith("NCHAR")) {
				return TDEngineDataType.TEXT;
			}
			throw new AssertionError(typeString);
		}
	}

	public static class TDEngineRowValue {

		private final TDEngineTables tables;
		private final Map<TDEngineColumn, TDEngineConstant> values;

		TDEngineRowValue(TDEngineTables tables, Map<TDEngineColumn, TDEngineConstant> values) {
			this.tables = tables;
			this.values = values;
		}

		public TDEngineTables getTable() {
			return tables;
		}

		public Map<TDEngineColumn, TDEngineConstant> getValues() {
			return values;
		}

		@Override
		public String toString() {
			StringBuffer sb = new StringBuffer();
			int i = 0;
			for (TDEngineColumn c : tables.getColumns()) {
				if (i++ != 0) {
					sb.append(", ");
				}
				sb.append(values.get(c));
			}
			return sb.toString();
		}

//		public String getRowValuesAsString() {
//			List<TDEngineColumn> columnsToCheck = tables.getColumns();
//			return getRowValuesAsString(columnsToCheck);
//		}

//		public String getRowValuesAsString(List<TDEngineColumn> columnsToCheck) {
//			StringBuilder sb = new StringBuilder();
//			Map<TDEngineColumn, TDEngineConstant> expectedValues = getValues();
//			for (int i = 0; i < columnsToCheck.size(); i++) {
//				if (i != 0) {
//					sb.append(", ");
//				}
//				TDEngineConstant expectedColumnValue = expectedValues.get(columnsToCheck.get(i));
//				TDEngineToStringVisitor visitor = new TDEngineToStringVisitor();
//				visitor.visit(expectedColumnValue);
//				sb.append(visitor.get());
//			}
//			return sb.toString();
//		}

	}

	public static class TDEngineTable implements Comparable<TDEngineTable> {

		private final String tableName;
		private final List<TDEngineColumn> columns;

		public TDEngineTable(String tableName, List<TDEngineColumn> columns) {
			this.tableName = tableName;
			this.columns = Collections.unmodifiableList(columns);
		}

		@Override
		public String toString() {
			StringBuffer sb = new StringBuffer();
			sb.append(tableName + "\n");
			for (TDEngineColumn c : columns) {
				sb.append("\t" + c + "\n");
			}
			return sb.toString();
		}

		public String getName() {
			return tableName;
		}

		public List<TDEngineColumn> getColumns() {
			return columns;
		}

		public String getColumnsAsString() {
			return columns.stream().map(c -> c.getName()).collect(Collectors.joining(", "));
		}

		public String getColumnsAsString(Function<TDEngineColumn, String> function) {
			return columns.stream().map(function).collect(Collectors.joining(", "));
		}

		public TDEngineColumn getRandomColumn() {
			return Randomly.fromList(columns);
		}

		@Override
		public int compareTo(TDEngineTable o) {
			return o.getName().compareTo(tableName);
		}

		public List<TDEngineColumn> getRandomNonEmptyColumnSubset() {
			return Randomly.nonEmptySubset(getColumns());
		}
		
		public List<TDEngineColumn> getRandomNonEmptyColumnSubsetWithFirstColumn() {
			List<TDEngineColumn> nonEmptySubset = Randomly.nonEmptySubset(getColumns());
			boolean containsFirstColumn = nonEmptySubset.get(0) == (columns.get(0));
			if (!containsFirstColumn) {
				nonEmptySubset.add(0, nonEmptySubset.get(0));
			}
			return nonEmptySubset;
		}

	}

	static public TDEngineSchema fromConnection(Connection con) throws SQLException {
		List<TDEngineTable> databaseTables = new ArrayList<>();
		try (Statement s = con.createStatement()) {
			try (ResultSet rs = s.executeQuery("show tables")) {
				List<String> tableNames = new ArrayList<>();
				while (rs.next()) {
					String tableName = rs.getString("table_name");
					tableNames.add(tableName);
				}
				for (String tableName : tableNames) {
					List<TDEngineColumn> databaseColumns = getTableColumns(con, tableName);
					TDEngineTable t = new TDEngineTable(tableName, databaseColumns);
					for (TDEngineColumn c : databaseColumns) {
						c.setTable(t);
					}
					databaseTables.add(t);
				}
			}
		}
		return new TDEngineSchema(databaseTables);
	}

	private static List<TDEngineColumn> getTableColumns(Connection con, String tableName) throws SQLException {
		List<TDEngineColumn> columns = new ArrayList<>();
		try (Statement s = con.createStatement()) {
			try (ResultSet rs = s.executeQuery("DESCRIBE " + tableName + "")) {
				while (rs.next()) {
					String columnName = rs.getString("Field");
					String dataType = rs.getString("Type");
					int length = rs.getInt("Length");
					TDEngineColumn c = new TDEngineColumn(columnName, getColumnType(dataType), length);
					columns.add(c);
				}
			}
		}
		return columns;
	}

	private final List<TDEngineTable> databaseTables;

	public TDEngineSchema(List<TDEngineTable> databaseTables) {
		this.databaseTables = Collections.unmodifiableList(databaseTables);
	}

	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		for (TDEngineTable t : getDatabaseTables()) {
			sb.append(t + "\n");
		}
		return sb.toString();
	}

	public TDEngineTable getRandomTable() {
		return Randomly.fromList(getDatabaseTables());
	}

	public TDEngineTables getRandomTableNonEmptyTables() {
		return new TDEngineTables(Randomly.nonEmptySubset(databaseTables));
	}

	public List<TDEngineTable> getDatabaseTables() {
		return databaseTables;
	}

	public List<TDEngineTable> getDatabaseTablesRandomSubsetNotEmpty() {
		return Randomly.nonEmptySubset(databaseTables);
	}

}
