package lama.tablegen.sqlite3;

import lama.Main.StateToReproduce;
import lama.Randomly;

public class SQLite3TableGenerator {

	private final StringBuilder sb = new StringBuilder();
	private final String tableName;
	private int columnId;
	private boolean containsPrimaryKey;

	public SQLite3TableGenerator(String tableName) {
		this.tableName = tableName;
	}

	public static String createTableStatement(String tableName, StateToReproduce state) {
		SQLite3TableGenerator sqLite3TableGenerator = new SQLite3TableGenerator(tableName);
		sqLite3TableGenerator.start();
		state.statements.add(sqLite3TableGenerator.sb.toString());
		return sqLite3TableGenerator.sb.toString();
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
		sb.append(")");
		if (containsPrimaryKey) {
			if (Randomly.getBoolean()) {
				// see https://sqlite.org/withoutrowid.html
				sb.append(" WITHOUT ROWID");
			}
		}
	}


	private enum Constraints {
		NONE, NOT_NULL, PRIMARY_KEY, UNIQUE
	}

	private void createColumn(boolean allowPrimaryKeyInColumn) {
		String columnName = String.format("c%d", columnId);
		sb.append(columnName);
		String dataType = Randomly.fromOptions(" INT", " TEXT", " BLOB", " REAL"); // TODO add INTEGER
		sb.append(dataType);
		boolean retry;
		do {
			retry = false;
			switch (Randomly.fromOptions(Constraints.values())) {
			case NONE:
				break;
			case PRIMARY_KEY:
				// only one primary key is allow if not specified as table constraint
				if (allowPrimaryKeyInColumn && !containsPrimaryKey) {
					sb.append(" PRIMARY KEY");
					containsPrimaryKey = true;
				} else {
					retry = true;
				}
				break;
			case UNIQUE:
				sb.append(" UNIQUE");
				break;
			case NOT_NULL:
				sb.append(" NOT NULL");
				break;
			default:
				throw new AssertionError();
			}
		} while (retry);
		if (Randomly.getBoolean()) {
			sb.append(SQLite3Common.getRandomCollate());
		}
	}

}
