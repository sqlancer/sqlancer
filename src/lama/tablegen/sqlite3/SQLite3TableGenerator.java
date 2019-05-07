package lama.tablegen.sqlite3;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import lama.Main.StateToReproduce;
import lama.Query;
import lama.QueryAdapter;
import lama.Randomly;
import lama.schema.Schema;
import lama.schema.Schema.Column;
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
	private boolean conflictClauseInserted;
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
			createColumn(allowPrimaryKeyInColumn);
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
			// FOREIGN KEY
			String randomColumn = Randomly.fromList(columnNames);
			sb.append(", FOREIGN KEY(");
			sb.append(randomColumn);
			sb.append(")");
			sb.append(" REFERENCES ");
			if (existingSchema.getDatabaseTables().isEmpty() || Randomly.getBooleanWithSmallProbability()) {
				String otherRandomColumn = Randomly.fromList(columnNames);
				sb.append(tableName + "(");
				sb.append(otherRandomColumn);
			} else {
				Table otherTable = existingSchema.getRandomTable();
				Column randomColumn2 = otherTable.getRandomColumn();
				sb.append(otherTable.getName());
				sb.append("(");
				sb.append(randomColumn2.getName());
				sb.append("");
			}
			sb.append(")");
		}

		if (Randomly.getBoolean()) {
			addCheckConstraint(); // TODO: incorporate columns
		}

		sb.append(")");
		if (containsPrimaryKey && !containsAutoIncrement) {
			if (Randomly.getBoolean()) {
				// see https://sqlite.org/withoutrowid.html
				sb.append(" WITHOUT ROWID");
			}
		}
	}

	private enum Constraints {
		NOT_NULL, PRIMARY_KEY, UNIQUE, CHECK
	}

	private void createColumn(boolean allowPrimaryKeyInColumn) {
		String columnName = SQLite3Common.createColumnName(columnId);
		columnNames.add(columnName);
		sb.append(columnName);
		sb.append(" ");
		String dataType = Randomly.fromOptions("INT", "TEXT", "BLOB", "REAL", "INTEGER");
		sb.append(dataType);

		if (Randomly.getBoolean()) {
			List<Constraints> constraints = Randomly.subset(Constraints.values());
			for (Constraints c : constraints) {
				switch (c) {
				case PRIMARY_KEY:
					// only one primary key is allow if not specified as table constraint
					if (allowPrimaryKeyInColumn && !containsPrimaryKey) {
						sb.append(" PRIMARY KEY");
						containsPrimaryKey = true;
						boolean hasOrdering = Randomly.getBoolean();
						if (hasOrdering) {
							if (Randomly.getBoolean()) {
								sb.append(" ASC");
							} else {
								sb.append(" DESC");
							}
						}
						if (Randomly.getBoolean()) {
							insertOnConflictClause();
						}
						if (!hasOrdering && dataType.equals("INTEGER")) {
							if (Randomly.getBoolean()) {
								containsAutoIncrement = true;
								sb.append(" AUTOINCREMENT");
							}
						}
					}
					break;
				case UNIQUE:
					sb.append(" UNIQUE");
					if (Randomly.getBoolean()) {
						insertOnConflictClause();
					}
					break;
				case NOT_NULL:
					sb.append(" NOT NULL");
					if (Randomly.getBoolean()) {
						insertOnConflictClause();
					}
					break;
				case CHECK:
					addCheckConstraint();
					break;
				default:
					throw new AssertionError();
				}
			}
		}
		if (Randomly.getBoolean()) {
			sb.append(" DEFAULT " + SQLite3Visitor.asString(SQLite3ExpressionGenerator.getRandomLiteralValue(false)));
		}
		if (Randomly.getBoolean()) {
			String randomCollate = SQLite3Common.getRandomCollate();
			sb.append(randomCollate);
		}
	}

	private void addCheckConstraint() {
		sb.append(" CHECK ( " + SQLite3Visitor.asString(SQLite3ExpressionGenerator.getRandomLiteralValue(false)) + ")");
	}

	// it seems that only one conflict clause can be inserted
	private void insertOnConflictClause() {
		if (!conflictClauseInserted) {
			sb.append(" ON CONFLICT ");
			sb.append(Randomly.fromOptions("ROLLBACK", "ABORT", "FAIL", "IGNORE", "REPLACE"));
			conflictClauseInserted = true;
		}
	}

}
