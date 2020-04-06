package sqlancer.clickhouse;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import sqlancer.Randomly;
import sqlancer.clickhouse.ClickhouseSchema.ClickhouseTable;
import sqlancer.schema.AbstractSchema;
import sqlancer.schema.AbstractTable;
import sqlancer.schema.AbstractTableColumn;
import sqlancer.schema.AbstractTables;
import sqlancer.schema.TableIndex;

public class ClickhouseSchema extends AbstractSchema<ClickhouseTable> {

	public static enum ClickhouseDataType {

		INT;

		public static ClickhouseDataType getRandom() {
			return Randomly.fromOptions(values());
		}

	}

	public static class ClickhouseCompositeDataType {

		private final ClickhouseDataType dataType;

		private final int size;

		public ClickhouseCompositeDataType(ClickhouseDataType dataType) {
			this.dataType = dataType;
			this.size = -1;
		}

		public ClickhouseCompositeDataType(ClickhouseDataType dataType, int size) {
			this.dataType = dataType;
			this.size = size;
		}

		public ClickhouseDataType getPrimitiveDataType() {
			return dataType;
		}

		public int getSize() {
			if (size == -1) {
				throw new AssertionError(this);
			}
			return size;
		}

		public static ClickhouseCompositeDataType getInt(int size) {
			return new ClickhouseCompositeDataType(ClickhouseDataType.INT, size);
		}

	}

	public static class ClickhouseColumn extends AbstractTableColumn<ClickhouseTable, ClickhouseCompositeDataType> {

		public ClickhouseColumn(String name, ClickhouseCompositeDataType columnType) {
			super(name, null, columnType);
		}

	}

	public static class ClickhouseTables extends AbstractTables<ClickhouseTable, ClickhouseColumn> {

		public ClickhouseTables(List<ClickhouseTable> tables) {
			super(tables);
		}

	}

	public ClickhouseSchema(List<ClickhouseTable> databaseTables) {
		super(databaseTables);
	}

	public ClickhouseTables getRandomTableNonEmptyTables() {
		return new ClickhouseTables(Randomly.nonEmptySubset(getDatabaseTables()));
	}

	private static ClickhouseCompositeDataType getColumnType(String typeString) {
		ClickhouseDataType primitiveType;
		switch (typeString) {
		case "Int32":
			primitiveType = ClickhouseDataType.INT;
			break;
		default:
			throw new AssertionError(typeString);
		}
		return new ClickhouseCompositeDataType(primitiveType);
	}

	public static class ClickhouseTable extends AbstractTable<ClickhouseColumn, TableIndex> {

		public ClickhouseTable(String tableName, List<ClickhouseColumn> columns, List<TableIndex> indexes, boolean isView) {
			super(tableName, columns, indexes, isView);
		}

	}

	public static ClickhouseSchema fromConnection(Connection con, String databaseName) throws SQLException {
		List<ClickhouseTable> databaseTables = new ArrayList<>();
		List<String> tableNames = getTableNames(con);
		for (String tableName : tableNames) {
			List<ClickhouseColumn> databaseColumns = getTableColumns(con, tableName);
			List<TableIndex> indexes = Collections.emptyList();
			boolean isView = tableName.startsWith("v");
			ClickhouseTable t = new ClickhouseTable(tableName, databaseColumns, indexes, isView);
			for (ClickhouseColumn c : databaseColumns) {
				c.setTable(t);
			}
			databaseTables.add(t);

		}
		return new ClickhouseSchema(databaseTables);
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

	private static List<ClickhouseColumn> getTableColumns(Connection con, String tableName) throws SQLException {
		List<ClickhouseColumn> columns = new ArrayList<>();
		try (Statement s = con.createStatement()) {
			try (ResultSet rs = s.executeQuery("DESCRIBE " + tableName)) {
				while (rs.next()) {
					String columnName = rs.getString("name");
					String dataType = rs.getString("type");
					ClickhouseColumn c = new ClickhouseColumn(columnName, getColumnType(dataType));
					columns.add(c);
				}
			}
		}
		return columns;
	}

}
