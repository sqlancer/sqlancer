package lama.tablegen.sqlite3;

import java.sql.Connection;
import java.sql.Statement;

import lama.Randomly;
import lama.Main.ReduceMeException;
import lama.Main.StateToReproduce;
import lama.schema.Schema.Table;
import lama.sqlite3.SQLite3Visitor;

public class SQLite3DeleteGenerator {

	public static void deleteContent(Table tableName, Connection con, StateToReproduce state) {
		try {
			try (Statement s = con.createStatement()) {
				String query = new SQLite3DeleteGenerator().getDeleteQuery(tableName);
				state.statements.add(query);
				s.execute(query);
			}
		} catch (Throwable e) {
			if (e.getMessage().startsWith("[SQLITE_ERROR] SQL error or missing database (integer overflow)")) {
				return;
			} else if (e.getMessage().startsWith("[SQLITE_ERROR] SQL error or missing database (foreign key mismatch")) {
				return;
			} else if (e.getMessage().startsWith("[SQLITE_CONSTRAINT]  Abort due to constraint violation (FOREIGN KEY constraint failed)")) {
				return;
			}
			state.logInconsistency(e);
			throw new ReduceMeException();
		}

	}

	private final StringBuilder sb = new StringBuilder();

	private String getDeleteQuery(Table tableName) {
		sb.append("DELETE FROM ");
		sb.append(tableName.getName());
		if (Randomly.getBoolean()) {
			sb.append(" WHERE ");
			sb.append(SQLite3Visitor
					.asString(SQLite3ExpressionGenerator.getRandomExpression(tableName.getColumns(), false)));
		}
		return sb.toString();
	}

}
