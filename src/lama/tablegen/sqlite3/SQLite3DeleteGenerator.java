package lama.tablegen.sqlite3;

import java.sql.Connection;
import java.sql.SQLException;

import lama.Main.StateToReproduce;
import lama.Query;
import lama.QueryAdapter;
import lama.Randomly;
import lama.schema.Schema.Table;
import lama.sqlite3.SQLite3Visitor;

public class SQLite3DeleteGenerator {

	public static Query deleteContent(Table tableName, Connection con, StateToReproduce state) {
		String query = new SQLite3DeleteGenerator().getDeleteQuery(tableName);
		return new QueryAdapter(query) {
			@Override
			public void execute(Connection con) throws SQLException {
				try {
					super.execute(con);
				} catch (Throwable e) {
					if (e.getMessage().startsWith("[SQLITE_ERROR] SQL error or missing database (integer overflow)")) {
						return;
					} else if (e.getMessage()
							.startsWith("[SQLITE_ERROR] SQL error or missing database (foreign key mismatch")) {
						return;
					} else if (e.getMessage().startsWith(
							"[SQLITE_CONSTRAINT]  Abort due to constraint violation (FOREIGN KEY constraint failed)")) {
						return;
					}
					throw e;
				}
			}
		};

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
