package lama.tablegen.sqlite3;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import lama.Main.StateToReproduce;
import lama.Query;
import lama.QueryAdapter;
import lama.Randomly;
import lama.schema.Schema;
import lama.schema.Schema.Table;
import lama.sqlite3.SQLite3Visitor;

/**
 * See https://www.sqlite.org/lang_createtable.html#rowid
 *
 * TODO What's missing:
 * <ul>
 * <li>TEMP tables (should be supported?)</li>
 * <li>create table as select</li>
 * <li>table constraints</li>
 * <li>foreign key constraints</li>
 * </ul>
 */
public class SQLite3TableGenerator {

	private final StringBuilder sb = new StringBuilder();
	private final String tableName;
	private int columnId;
	private boolean containsPrimaryKey;
	private boolean containsAutoIncrement;
	private final List<String> columnNames = new ArrayList<>();
	private final Schema existingSchema;

	public SQLite3TableGenerator(String tableName, Schema existingSchema) {
		this.tableName = tableName;
		this.existingSchema = existingSchema;
	}

	public static Query createTableStatement(String tableName, StateToReproduce state, Schema existingSchema) {
		SQLite3TableGenerator sqLite3TableGenerator = new SQLite3TableGenerator(tableName, existingSchema);
		sqLite3TableGenerator.start();
		return new QueryAdapter(sqLite3TableGenerator.sb.toString());
	}

	public void start() {
		sb.append("CREATE ");
//		if (Randomly.getBoolean()) {
//			if (Randomly.getBoolean()) {
//				sb.append("TEMP ");
//			} else {
//				sb.append("TEMPORARY ");
//			}
//		}
		sb.append("TABLE ");
		if (Randomly.getBoolean()) {
			sb.append("IF NOT EXISTS ");
		}
		sb.append(tableName + " ");
		sb.append("(");
		boolean allowPrimaryKeyInColumn = Randomly.getBoolean();
		for (int i = 0; i < 3 + Randomly.smallNumber(); i++) {
			if (i != 0) {
				sb.append(", ");
			}
			String columnName = SQLite3Common.createColumnName(columnId);
			SQLite3ColumnBuilder columnBuilder = new SQLite3ColumnBuilder().allowPrimaryKey(allowPrimaryKeyInColumn && !containsPrimaryKey);
			sb.append(columnBuilder.createColumn(columnName));
			sb.append(" ");
			if (columnBuilder.isContainsAutoIncrement()) {
				this.containsAutoIncrement = true;
			}
			if (columnBuilder.isContainsPrimaryKey()) {
				this.containsPrimaryKey = true;
			}
			
			columnNames.add(columnName);
			columnId++;
		}
		if (!containsPrimaryKey && Randomly.getBoolean()) {
			// TODO check constraints?
			List<String> selectedColumns = Randomly.nonEmptySubset(columnNames);
			sb.append(", PRIMARY KEY (");
			sb.append(selectedColumns.stream().collect(Collectors.joining(", ")));
			sb.append(")");
			containsPrimaryKey = true;
		}

		if (Randomly.getBoolean()) {
			addForeignKey();
		}

		if (Randomly.getBoolean()) {
			sb.append(SQLite3Common.getCheckConstraint()); // TODO: incorporate columns
		}

		sb.append(")");
		if (containsPrimaryKey && !containsAutoIncrement) {
			if (Randomly.getBoolean()) {
				// see https://sqlite.org/withoutrowid.html
				sb.append(" WITHOUT ROWID");
			}
		}
	}

	/**
	 * @see https://www.sqlite.org/foreignkeys.html
	 */
	private void addForeignKey() {
		List<String> foreignKeyColumns = new ArrayList<>();
		if (Randomly.getBoolean()) {
			foreignKeyColumns = Arrays.asList(Randomly.fromList(columnNames));
		} else {
			foreignKeyColumns = new ArrayList<>();
			do {
				foreignKeyColumns.add(Randomly.fromList(columnNames));
			} while (Randomly.getBoolean());
		}
		sb.append(", FOREIGN KEY(");
		sb.append(foreignKeyColumns.stream().collect(Collectors.joining(", ")));
		sb.append(")");
		sb.append(" REFERENCES ");
		String referencedTableName;
		List<String> columns = new ArrayList<>();
		if (existingSchema.getDatabaseTables().isEmpty() || Randomly.getBooleanWithSmallProbability()) {
			// the foreign key references our own table
			referencedTableName = tableName;
			for (int i = 0; i < foreignKeyColumns.size(); i++) {
				columns.add(Randomly.fromList(columnNames));
			}
		} else {
			Table randomTable = existingSchema.getRandomTable();
			referencedTableName = randomTable.getName();
			for (int i = 0; i < foreignKeyColumns.size(); i++) {
				columns.add(randomTable.getRandomColumn().getName());
			}
		}
		sb.append(referencedTableName);
		sb.append("(");
		sb.append(columns.stream().collect(Collectors.joining(", ")));
		sb.append(")");
		addActionClause(" ON DELETE ");
		addActionClause(" ON UPDATE ");
		if (Randomly.getBoolean()) {
			// add a deferrable clause
			sb.append(" ");
			String deferrable = Randomly.fromOptions("DEFERRABLE INITIALLY DEFERRED",
					"NOT DEFERRABLE INITIALLY DEFERRED", "NOT DEFERRABLE INITIALLY IMMEDIATE", "NOT DEFERRABLE",
					"DEFERRABLE INITIALLY IMMEDIATE", "DEFERRABLE");
			sb.append(deferrable);
		}
	}

	private void addActionClause(String string) {
		if (Randomly.getBoolean()) {
			// add an ON DELETE or ON ACTION clause
			sb.append(string);
			sb.append(Randomly.fromOptions("NO ACTION", "RESTRICT", "SET NULL", "SET DEFAULT", "CASCADE"));
		}
	}




}
