package lama.sqlite3.gen;

import java.sql.Connection;
import java.sql.SQLException;

import lama.Main.StateToReproduce;
import lama.Query;
import lama.QueryAdapter;
import lama.Randomly;
import lama.sqlite3.SQLite3Visitor;
import lama.sqlite3.schema.SQLite3Schema.Table;

public class SQLite3DeleteGenerator {

	private final Randomly r;

	public SQLite3DeleteGenerator(Randomly r) {
		this.r = r;
	}

	public static Query deleteContent(Table tableName, Connection con, StateToReproduce state, Randomly r) {
		String query = new SQLite3DeleteGenerator(r).getDeleteQuery(tableName);
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
					} else if (e.getMessage().startsWith("[SQLITE_CONSTRAINT]  Abort due to constraint violation ")) {
						return;
					} else if (e.getMessage()
							.startsWith("[SQLITE_ERROR] SQL error or missing database (parser stack overflow)")) {
						return;
					} else if (e.getMessage().startsWith(
							"[SQLITE_ERROR] SQL error or missing database (second argument to likelihood() must be a constant between 0.0 and 1.0)")) {
						return;
					} else if (e.getMessage()
							.startsWith("[SQLITE_ERROR] SQL error or missing database (no such table:")) {
						return; // TODO: also check if the table is really missing (caused by a DROP TABLE)
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
					.asString(new SQLite3ExpressionGenerator().getRandomExpression(tableName.getColumns(), false, r)));
		}
		return sb.toString();
	}

}
