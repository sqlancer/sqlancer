package lama.sqlite3.gen;

import java.util.List;

import lama.Randomly;
import lama.sqlite3.SQLite3Visitor;

public class SQLite3ColumnBuilder {

	private boolean containsPrimaryKey;
	private boolean containsAutoIncrement;
	private final StringBuilder sb = new StringBuilder();
	private boolean conflictClauseInserted;

	private boolean allowPrimaryKey = true;
	private boolean allowUnique = true;
	private boolean allowDefaultValue = true;
	private boolean allowCheck = true;
	private boolean allowNotNull = true;

	private enum Constraints {
		NOT_NULL, PRIMARY_KEY, UNIQUE, CHECK
	}

	public boolean isContainsAutoIncrement() {
		return containsAutoIncrement;
	}

	public boolean isConflictClauseInserted() {
		return conflictClauseInserted;
	}

	public boolean isContainsPrimaryKey() {
		return containsPrimaryKey;
	}

	String createColumn(String columnName, Randomly r) {
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
					if (allowPrimaryKey) {
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
					if (allowUnique) {
						sb.append(" UNIQUE");
						if (Randomly.getBoolean()) {
							insertOnConflictClause();
						}
					}
					break;
				case NOT_NULL:
					if (allowNotNull) {
						sb.append(" NOT NULL");
						if (Randomly.getBoolean()) {
							insertOnConflictClause();
						}
					}
					break;
				case CHECK:
					if (allowCheck) {
						sb.append(SQLite3Common.getCheckConstraint(r));
					}
					break;
				default:
					throw new AssertionError();
				}
			}
		}
		if (allowDefaultValue && Randomly.getBoolean()) {
			sb.append(" DEFAULT " + SQLite3Visitor.asString(SQLite3ExpressionGenerator.getRandomLiteralValue(false, r)));
		}
		if (Randomly.getBoolean() && false /* FIXME: for view testing temporarily disabled */) {
			String randomCollate = SQLite3Common.getRandomCollate();
			sb.append(randomCollate);
		}
		return sb.toString();
	}

	// it seems that only one conflict clause can be inserted
	private void insertOnConflictClause() {
		if (!conflictClauseInserted) {
			sb.append(" ON CONFLICT ");
			sb.append(Randomly.fromOptions("ROLLBACK", "ABORT", "FAIL", "IGNORE", "REPLACE"));
			conflictClauseInserted = true;
		}
	}

	public SQLite3ColumnBuilder allowPrimaryKey(boolean allowPrimaryKey) {
		this.allowPrimaryKey = allowPrimaryKey;
		return this;
	}

	public SQLite3ColumnBuilder allowUnique(boolean allowUnique) {
		this.allowUnique = allowUnique;
		return this;
	}

	public SQLite3ColumnBuilder allowDefaultValue(boolean allowDefaultValue) {
		this.allowDefaultValue = allowDefaultValue;
		return this;
	}

	public SQLite3ColumnBuilder allowCheck(boolean allowCheck) {
		this.allowCheck = allowCheck;
		return this;
	}

	public SQLite3ColumnBuilder allowNotNull(boolean allowNotNull) {
		this.allowNotNull = allowNotNull;
		return this;
	}

}
