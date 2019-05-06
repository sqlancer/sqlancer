package lama.tablegen.sqlite3;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import lama.Main.StateToReproduce;
import lama.Randomly;
import lama.schema.Schema;
import lama.schema.Schema.Column;
import lama.schema.Schema.Table;

public class SQLite3AlterTable {

	private enum Option {
		RENAME_TABLE, RENAME_COLUMN
	}

	public static void alterTable(Schema s, Connection con, StateToReproduce state) throws SQLException {
		SQLite3AlterTable alterTable = new SQLite3AlterTable();
		// TODO implement add column, which has many constraints
		Option option = Randomly.fromOptions(Option.values());
		switch (option) {
		case RENAME_TABLE:
			alterTable.renameTable(s);
			break;
		case RENAME_COLUMN:
			alterTable.renameColumn(s);
			break;
		default:
			throw new AssertionError();
		}

		try (Statement st = con.createStatement()) {
			String query = alterTable.sb.toString();
			state.statements.add(query);
			st.execute(query);
		}
	}

	private void renameColumn(Schema s) {
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

	private final StringBuilder sb = new StringBuilder();

	private void renameTable(Schema s) {
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
