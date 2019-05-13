package lama.sqlite3.gen;

import java.sql.Connection;
import java.sql.SQLException;

import org.sqlite.SQLiteException;

import lama.Main.StateToReproduce;
import lama.sqlite3.schema.SQLite3Schema;
import lama.sqlite3.schema.SQLite3Schema.Column;
import lama.sqlite3.schema.SQLite3Schema.Table;
import lama.Query;
import lama.QueryAdapter;
import lama.Randomly;

public class SQLite3AlterTable {

	private final Randomly r;

	private enum Option {
		RENAME_TABLE, RENAME_COLUMN, ADD_COLUMN
	}

	public SQLite3AlterTable(Randomly r) {
		this.r = r;
	}

	public static Query alterTable(SQLite3Schema s, Connection con, StateToReproduce state, Randomly r) throws SQLException {
		SQLite3AlterTable alterTable = new SQLite3AlterTable(r);
		Option option = Randomly.fromOptions(Option.values());
		switch (option) {
		case RENAME_TABLE:
			alterTable.renameTable(s);
			return new QueryAdapter(alterTable.sb.toString());
		case RENAME_COLUMN:
			alterTable.renameColumn(s);
			return new QueryAdapter(alterTable.sb.toString());
		case ADD_COLUMN:
			alterTable.addColumn(s);
			return new QueryAdapter(alterTable.sb.toString()) {
				@Override
				public void execute(Connection con) throws SQLException {
					try {
						super.execute(con);
					} catch (SQLiteException e) {
						if (e.getMessage().equals("[SQLITE_ERROR] SQL error or missing database (Cannot add a NOT NULL column with default value NULL)")) {
							return;
						} else {
							throw e;
						}
					}
				}
			};
		default:
			throw new AssertionError();
		}
		

	}

	private void renameColumn(SQLite3Schema s) {
		Table t = s.getRandomTable();
		Column c = t.getRandomColumn();
		sb.append("ALTER TABLE ");
		sb.append(t.getName());
		sb.append(" RENAME COLUMN ");
		sb.append(c.getName());
		sb.append(" TO ");
		int nr = 0;
		String[] name = new String[1];
		do {
			name[0] = SQLite3Common.createColumnName(nr++);
		} while (t.getColumns().stream().anyMatch(col -> col.getName().contentEquals(name[0])));
		sb.append(name[0]);
	}
	
	private void addColumn(SQLite3Schema s) {
		Table t = s.getRandomTable();
		sb.append("ALTER TABLE ");
		sb.append(t.getName());
		sb.append(" ADD COLUMN ");
		int nr = 0;
		String[] name = new String[1];
		do {
			name[0] = SQLite3Common.createColumnName(nr++);
		} while (t.getColumns().stream().anyMatch(col -> col.getName().contentEquals(name[0])));
		// The column may not have a PRIMARY KEY or UNIQUE constraint.
		// The column may not have a default value of CURRENT_TIME, CURRENT_DATE, CURRENT_TIMESTAMP, or an expression in parentheses.
		// If a NOT NULL constraint is specified, then the column must have a default value other than NULL.
		// If foreign key constraints are enabled and a column with a REFERENCES clause is added, the column must have a default value of NULL.
		sb.append(new SQLite3ColumnBuilder().allowPrimaryKey(false).allowUnique(false).allowNotNull(false).allowDefaultValue(false).createColumn(name[0], r));
	} 

	private final StringBuilder sb = new StringBuilder();

	private void renameTable(SQLite3Schema s) {
		Table t = s.getRandomTable();
		sb.append("ALTER TABLE ");
		sb.append(t.getName());
		sb.append(" RENAME TO ");
		int nr = 0;
		String[] name = new String[1];
		do {
			name[0] = SQLite3Common.createTableName(nr++);
		} while (s.getDatabaseTables().stream().anyMatch(tab -> tab.getName().contentEquals(name[0])));
		sb.append(name[0]);
	}

}
