package lama.cockroachdb;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import lama.IgnoreMeException;
import lama.Randomly;

public class CockroachDBSchema {

	public static enum CockroachDBDataType {
		
		
		INT, BOOL, STRING, FLOAT, BYTES, BIT, VARBIT, SERIAL;
		
		private CockroachDBDataType() {
			isPrimitive = true;
		}

		private CockroachDBDataType(boolean isPrimitive) {
			this.isPrimitive = isPrimitive;
		}
		
		private final boolean isPrimitive;

		public static CockroachDBDataType getRandom() {
			return Randomly.fromOptions(values());
		}

		public CockroachDBCompositeDataType get() {
			return CockroachDBCompositeDataType.getRandomForType(this);
		}
		
		public boolean isPrimitive() {
			return isPrimitive;
		}
	}
	
	public static class CockroachDBCompositeDataType {
		
		private final CockroachDBDataType dataType;

		private final int size;
		
		public CockroachDBCompositeDataType(CockroachDBDataType dataType) {
			this.dataType = dataType;
			this.size = -1;
		}
		
		public CockroachDBCompositeDataType(CockroachDBDataType dataType, int size) {
			this.dataType = dataType;
			this.size = size;
		}
		
		
		public CockroachDBDataType getPrimitiveDataType() {
			return dataType;
		}
		
		public int getSize() {
			if (size == -1) {
				throw new AssertionError(this);
			}
			return size;
		}

		public boolean isString() {
			return dataType == CockroachDBDataType.STRING;
		}
		
		public static CockroachDBCompositeDataType getInt(int size) {
			return new CockroachDBCompositeDataType(CockroachDBDataType.INT, size);
		}
		
		public static CockroachDBCompositeDataType getBit(int size) {
			return new CockroachDBCompositeDataType(CockroachDBDataType.BIT, size);
		}
		
		@Override
		public String toString() {
			switch (dataType) {
			case INT:
				switch (size) {
				case 2:
					return Randomly.fromOptions("INT2", "SMALLINT");
				case 4:
					return "INT4";
				case 8:
					// "INTEGER": can be affected by a session variable
					return Randomly.fromOptions("INT8", "INT64", "BIGINT");
				default:
					return "INT";
				}
			case SERIAL:
				switch (size) {
				case 2:
					return Randomly.fromOptions("SERIAL2", "SMALLSERIAL");
				case 4:
					return "SERIAL4";
				case 8:
					return Randomly.fromOptions("SERIAL8", "BIGSERIAL");
				default:
					throw new AssertionError();
				}
			case BIT:
				if (size == 1 && Randomly.getBoolean()) {
					return "BIT";
				} else {
					return String.format("BIT(%d)", size);
				}
			case VARBIT:
				if (size == -1) {
					return String.format("VARBIT");
				} else {
					return String.format("VARBIT(%d)", size);
				}
				default:
					return dataType.toString();
			}
		}

		public static CockroachDBCompositeDataType getRandom() {
			CockroachDBDataType randomDataType = CockroachDBDataType.getRandom();
			return getRandomForType(randomDataType);
		}

		private static CockroachDBCompositeDataType getRandomForType(CockroachDBDataType randomDataType) {
			if (randomDataType == CockroachDBDataType.INT || randomDataType == CockroachDBDataType.SERIAL) {
				return new CockroachDBCompositeDataType(randomDataType, Randomly.fromOptions(2, 4, 8));
			} else if (randomDataType == CockroachDBDataType.BIT) {
				return new CockroachDBCompositeDataType(randomDataType, (int) Randomly.getNotCachedInteger(1, 200));
			} else if (randomDataType == CockroachDBDataType.VARBIT) {
				return new CockroachDBCompositeDataType(randomDataType, (int) Randomly.getNotCachedInteger(1, 200));
			} else {
				return new CockroachDBCompositeDataType(randomDataType);
			}
		}

		public static CockroachDBCompositeDataType getVarBit(int maxSize) {
			return new CockroachDBCompositeDataType(CockroachDBDataType.VARBIT, maxSize);
		}
		
	}

	public static class CockroachDBColumn implements Comparable<CockroachDBColumn> {

		private final String name;
		private final CockroachDBCompositeDataType columnType;
		private final boolean isPrimaryKey;
		private CockroachDBTable table;
		private boolean isNullable;

		public CockroachDBColumn(String name, CockroachDBCompositeDataType columnType, boolean isPrimaryKey,
				boolean isNullable) {
			this.name = name;
			this.columnType = columnType;
			this.isPrimaryKey = isPrimaryKey;
			this.isNullable = isNullable;
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
			if (!(obj instanceof CockroachDBColumn)) {
				return false;
			} else {
				CockroachDBColumn c = (CockroachDBColumn) obj;
				return table.getName().contentEquals(getName()) && name.equals(c.name);
			}
		}

		public String getName() {
			return name;
		}

		public String getFullQualifiedName() {
			return table.getName() + "." + getName();
		}

		public CockroachDBCompositeDataType getColumnType() {
			return columnType;
		}

		public boolean isPrimaryKey() {
			return isPrimaryKey;
		}

		public void setTable(CockroachDBTable t) {
			this.table = t;
		}

		public CockroachDBTable getTable() {
			return table;
		}

		@Override
		public int compareTo(CockroachDBColumn o) {
			if (o.getTable().equals(this.getTable())) {
				return name.compareTo(o.getName());
			} else {
				return o.getTable().compareTo(table);
			}
		}

		public boolean isNullable() {
			return isNullable;
		}

	}

	public static class CockroachDBTables {

		private final List<CockroachDBTable> tables;
		private final List<CockroachDBColumn> columns;

		public CockroachDBTables(List<CockroachDBTable> tables) {
			this.tables = tables;
			columns = new ArrayList<>();
			for (CockroachDBTable t : tables) {
				columns.addAll(t.getColumns());
			}
		}

		public String tableNamesAsString() {
			return tables.stream().map(t -> t.getName()).collect(Collectors.joining(", "));
		}

		public List<CockroachDBTable> getTables() {
			return tables;
		}

		public List<CockroachDBColumn> getColumns() {
			return columns;
		}

		public String columnNamesAsString() {
			return getColumns().stream().map(t -> t.getTable().getName() + "." + t.getName())
					.collect(Collectors.joining(", "));
		}

		public String columnNamesAsString(Function<CockroachDBColumn, String> function) {
			return getColumns().stream().map(function).collect(Collectors.joining(", "));
		}
	}

	private static CockroachDBCompositeDataType getColumnType(String typeString) {
		if (typeString.startsWith("STRING COLLATE")) {
			return new CockroachDBCompositeDataType(CockroachDBDataType.STRING);
		}
		if (typeString.startsWith("BIT(")) {
			int val = Integer.valueOf(typeString.substring(4, typeString.length() - 1));
			return CockroachDBCompositeDataType.getBit(val);
		}
		if (typeString.startsWith("VARBIT(")) {
			int val = Integer.valueOf(typeString.substring(7, typeString.length() - 1));
			return CockroachDBCompositeDataType.getBit(val);
		}
		switch (typeString) {
		case "VARBIT":
			return CockroachDBCompositeDataType.getVarBit(-1);
		case "BIT":
			return CockroachDBCompositeDataType.getBit(1);
		case "INT8":
			return CockroachDBCompositeDataType.getInt(8);
		case "INT4":
			return CockroachDBCompositeDataType.getInt(4);
		case "INT2":
			return CockroachDBCompositeDataType.getInt(2);
		case "BOOL":
			return new CockroachDBCompositeDataType(CockroachDBDataType.BOOL);
		case "STRING":
			return new CockroachDBCompositeDataType(CockroachDBDataType.STRING);
		case "FLOAT8":
			return new CockroachDBCompositeDataType(CockroachDBDataType.FLOAT);
		case "BYTES":
			return new CockroachDBCompositeDataType(CockroachDBDataType.BYTES);
		case "DECIMAL": // TODO
			throw new IgnoreMeException();
		default:
			throw new AssertionError(typeString);
		}
	}

	public static class CockroachDBTable implements Comparable<CockroachDBTable> {

		private final String tableName;
		private final List<CockroachDBColumn> columns;
		private final List<CockroachDBIndex> indexes;
		private final boolean isView;

		public CockroachDBTable(String tableName, List<CockroachDBColumn> columns, List<CockroachDBIndex> indexes, boolean isView) {
			this.tableName = tableName;
			this.indexes = indexes;
			this.isView = isView;
			this.columns = Collections.unmodifiableList(columns);
		}

		@Override
		public String toString() {
			StringBuffer sb = new StringBuffer();
			sb.append(tableName + "\n");
			for (CockroachDBColumn c : columns) {
				sb.append("\t" + c + "\n");
			}
			return sb.toString();
		}

		public List<CockroachDBIndex> getIndexes() {
			return indexes;
		}

		public String getName() {
			return tableName;
		}

		public List<CockroachDBColumn> getColumns() {
			return columns;
		}

		public String getColumnsAsString() {
			return columns.stream().map(c -> c.getName()).collect(Collectors.joining(", "));
		}

		public String getColumnsAsString(Function<CockroachDBColumn, String> function) {
			return columns.stream().map(function).collect(Collectors.joining(", "));
		}

		public CockroachDBColumn getRandomColumn() {
			return Randomly.fromList(columns);
		}

		public boolean hasIndexes() {
			return !indexes.isEmpty();
		}

		public CockroachDBIndex getRandomIndex() {
			return Randomly.fromList(indexes);
		}

		@Override
		public int compareTo(CockroachDBTable o) {
			return o.getName().compareTo(tableName);
		}

		public List<CockroachDBColumn> getRandomNonEmptyColumnSubset() {
			return Randomly.nonEmptySubset(getColumns());
		}

		public boolean hasPrimaryKey() {
			return columns.stream().anyMatch(c -> c.isPrimaryKey());
		}
		
		public boolean isView() {
			return isView;
		}
	}

	public static final class CockroachDBIndex {

		private final String indexName;

		private CockroachDBIndex(String indexName) {
			this.indexName = indexName;
		}

		public static CockroachDBIndex create(String indexName) {
			return new CockroachDBIndex(indexName);
		}

		public String getIndexName() {
			return indexName;
		}

	}

	public static CockroachDBSchema fromConnection(Connection con, String databaseName) throws SQLException {
		List<CockroachDBTable> databaseTables = new ArrayList<>();
		List<String> tableNames = getTableNames(con);
		for (String tableName : tableNames) {
			List<CockroachDBColumn> databaseColumns = getTableColumns(con, tableName);
			List<CockroachDBIndex> indexes = getIndexes(con, tableName, databaseName);
			boolean isView = tableName.startsWith("v");
			CockroachDBTable t = new CockroachDBTable(tableName, databaseColumns, indexes, isView);
			for (CockroachDBColumn c : databaseColumns) {
				c.setTable(t);
			}
			databaseTables.add(t);

		}
		return new CockroachDBSchema(databaseTables);
	}

	private static List<String> getTableNames(Connection con) throws SQLException {
		List<String> tableNames = new ArrayList<>();
		try (Statement s = con.createStatement()) {
			ResultSet tableRs = s.executeQuery("SHOW TABLES");
			while (tableRs.next()) {
				String tableName = tableRs.getString(1);
		
				tableNames.add(tableName);
			}
		}
		return tableNames;
	}

	private static List<CockroachDBIndex> getIndexes(Connection con, String tableName, String databaseName)
			throws SQLException {
		List<CockroachDBIndex> indexes = new ArrayList<>();
		try (Statement s = con.createStatement()) {
			try (ResultSet rs = s.executeQuery(String.format("SHOW INDEX FROM %s", tableName))) {
				while (rs.next()) {
					String indexName = rs.getString("index_name");
					indexes.add(CockroachDBIndex.create(indexName));
				}
			}
		}
		return indexes;
	}

	private static List<CockroachDBColumn> getTableColumns(Connection con, String tableName) throws SQLException {
		List<CockroachDBColumn> columns = new ArrayList<>();
		try (Statement s = con.createStatement()) {
			try (ResultSet rs = s.executeQuery("SHOW COLUMNS FROM " + tableName)) {
				while (rs.next()) {
					String columnName = rs.getString("column_name");
					String dataType = rs.getString("data_type");
					boolean isNullable = rs.getBoolean("is_nullable");
					String indices = rs.getString("indices");
					boolean isPrimaryKey = indices.contains("primary");
					CockroachDBColumn c = new CockroachDBColumn(columnName, getColumnType(dataType), isPrimaryKey,
							isNullable);
					columns.add(c);
				}
			}
		}
		return columns;
	}

	private final List<CockroachDBTable> databaseTables;

	public CockroachDBSchema(List<CockroachDBTable> databaseTables) {
		this.databaseTables = Collections.unmodifiableList(databaseTables);
	}

	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		for (CockroachDBTable t : getDatabaseTables()) {
			sb.append(t + "\n");
		}
		return sb.toString();
	}

	public CockroachDBTable getRandomTable() {
		return Randomly.fromList(getDatabaseTables());
	}
	
	public CockroachDBTable getRandomTable(Predicate<CockroachDBTable> predicate) {
		return Randomly.fromList(getDatabaseTables().stream().filter(predicate).collect(Collectors.toList()));
	}

	public CockroachDBTables getRandomTableNonEmptyTables() {
		return new CockroachDBTables(Randomly.nonEmptySubset(databaseTables));
	}

	public List<CockroachDBTable> getDatabaseTables() {
		return databaseTables;
	}

	public List<CockroachDBTable> getDatabaseTablesRandomSubsetNotEmpty() {
		return Randomly.nonEmptySubset(databaseTables);
	}

}
