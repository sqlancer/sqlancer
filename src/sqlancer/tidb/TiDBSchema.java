package sqlancer.tidb;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import sqlancer.Randomly;
import sqlancer.schema.AbstractSchema;
import sqlancer.schema.AbstractTable;
import sqlancer.schema.AbstractTableColumn;
import sqlancer.schema.AbstractTables;
import sqlancer.schema.TableIndex;
import sqlancer.tidb.TiDBSchema.TiDBTable;

public class TiDBSchema extends AbstractSchema<TiDBTable> {

	public static enum TiDBDataType {

		INT, TEXT, BOOL, FLOAT, DOUBLE;

		private TiDBDataType() {
			isPrimitive = true;
		}

		private TiDBDataType(boolean isPrimitive) {
			this.isPrimitive = isPrimitive;
		}

		private final boolean isPrimitive;

		public static TiDBDataType getRandom() {
			return Randomly.fromOptions(values());
		}

		public boolean isPrimitive() {
			return isPrimitive;
		}
	}

	public static class TiDBCompositeDataType {

		private final TiDBDataType dataType;

		private final int size;

		public TiDBCompositeDataType(TiDBDataType dataType) {
			this.dataType = dataType;
			this.size = -1;
		}

		public TiDBCompositeDataType(TiDBDataType dataType, int size) {
			this.dataType = dataType;
			this.size = size;
		}

		public TiDBDataType getPrimitiveDataType() {
			return dataType;
		}

		public int getSize() {
			if (size == -1) {
				throw new AssertionError(this);
			}
			return size;
		}

		public static TiDBCompositeDataType getInt(int size) {
			return new TiDBCompositeDataType(TiDBDataType.INT, size);
		}

	}

	public static class TiDBColumn extends AbstractTableColumn<TiDBTable, TiDBCompositeDataType> {

		private final boolean isPrimaryKey;
		private boolean isNullable;

		public TiDBColumn(String name, TiDBCompositeDataType columnType, boolean isPrimaryKey, boolean isNullable) {
			super(name, null, columnType);
			this.isPrimaryKey = isPrimaryKey;
			this.isNullable = isNullable;
		}

		public boolean isPrimaryKey() {
			return isPrimaryKey;
		}

		public boolean isNullable() {
			return isNullable;
		}

	}

	public static class TiDBTables extends AbstractTables<TiDBTable, TiDBColumn> {

		public TiDBTables(List<TiDBTable> tables) {
			super(tables);
		}

	}

	public TiDBSchema(List<TiDBTable> databaseTables) {
		super(databaseTables);
	}

	public TiDBTables getRandomTableNonEmptyTables() {
		return new TiDBTables(Randomly.nonEmptySubset(getDatabaseTables()));
	}

	private static TiDBCompositeDataType getColumnType(String typeString) {
		if (typeString.startsWith("int") || typeString.startsWith("bigint") || typeString.contains("decimal")) {
			return new TiDBCompositeDataType(TiDBDataType.INT);
		}
		if (typeString.startsWith("var_string") || typeString.contains("binary")) {
			return new TiDBCompositeDataType(TiDBDataType.TEXT);
		}
		TiDBDataType primitiveType;
		switch (typeString) {
		case "text":
		case "longtext":
			primitiveType = TiDBDataType.TEXT;
			break;
		case "float":
		case "double":
			primitiveType = TiDBDataType.FLOAT;
			break;
		case "tinyint(1)":
			primitiveType = TiDBDataType.BOOL;
			break;
		case "null":
			primitiveType = TiDBDataType.INT;
			break;
		default:
			throw new AssertionError(typeString);
		}
		return new TiDBCompositeDataType(primitiveType);
	}

	public static class TiDBTable extends AbstractTable<TiDBColumn, TableIndex> {

		public TiDBTable(String tableName, List<TiDBColumn> columns, List<TableIndex> indexes, boolean isView) {
			super(tableName, columns, indexes, isView);
		}

		public boolean hasPrimaryKey() {
			return getColumns().stream().anyMatch(c -> c.isPrimaryKey());
		}

	}

	public static TiDBSchema fromConnection(Connection con, String databaseName) throws SQLException {
		List<TiDBTable> databaseTables = new ArrayList<>();
		List<String> tableNames = getTableNames(con);
		for (String tableName : tableNames) {
			List<TiDBColumn> databaseColumns = getTableColumns(con, tableName);
			List<TableIndex> indexes = getIndexes(con, tableName, databaseName);
			boolean isView = tableName.startsWith("v");
			TiDBTable t = new TiDBTable(tableName, databaseColumns, indexes, isView);
			for (TiDBColumn c : databaseColumns) {
				c.setTable(t);
			}
			databaseTables.add(t);

		}
		return new TiDBSchema(databaseTables);
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

	private static List<TableIndex> getIndexes(Connection con, String tableName, String databaseName)
			throws SQLException {
		List<TableIndex> indexes = new ArrayList<>();
		try (Statement s = con.createStatement()) {
			try (ResultSet rs = s.executeQuery(String.format("SHOW INDEX FROM %s", tableName))) {
				while (rs.next()) {
					String indexName = rs.getString("Key_name");
					indexes.add(TableIndex.create(indexName));
				}
			}
		}
		return indexes;
	}

	private static List<TiDBColumn> getTableColumns(Connection con, String tableName) throws SQLException {
		List<TiDBColumn> columns = new ArrayList<>();
		try (Statement s = con.createStatement()) {
			try (ResultSet rs = s.executeQuery("SHOW COLUMNS FROM " + tableName)) {
				while (rs.next()) {
					String columnName = rs.getString("Field");
					String dataType = rs.getString("Type");
					boolean isNullable = rs.getString("Null").contentEquals("YES");
					boolean isPrimaryKey = rs.getString("Key").contains("PRI");
					TiDBColumn c = new TiDBColumn(columnName, getColumnType(dataType), isPrimaryKey, isNullable);
					columns.add(c);
				}
			}
		}
		return columns;
	}

}
