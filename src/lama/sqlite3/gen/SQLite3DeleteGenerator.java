package lama.sqlite3.gen;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;

import lama.Query;
import lama.QueryAdapter;
import lama.Randomly;
import lama.StateToReproduce;
import lama.sqlite3.SQLite3Visitor;
import lama.sqlite3.schema.SQLite3Schema.Table;

public class SQLite3DeleteGenerator {

	private final Randomly r;

	public SQLite3DeleteGenerator(Randomly r) {
		this.r = r;
	}

	public static Query deleteContent(Table tableName, Connection con, StateToReproduce state, Randomly r) {
		String query = new SQLite3DeleteGenerator(r).getDeleteQuery(tableName);
		return new QueryAdapter(query, Arrays.asList("[SQLITE_ERROR] SQL error or missing database (integer overflow)",
				"[SQLITE_ERROR] SQL error or missing database (foreign key mismatch",
				"[SQLITE_CONSTRAINT]  Abort due to constraint violation ",
				"[SQLITE_ERROR] SQL error or missing database (parser stack overflow)",
				"[SQLITE_ERROR] SQL error or missing database (second argument to likelihood() must be a constant between 0.0 and 1.0)",
				"[SQLITE_ERROR] SQL error or missing database (no such table:")) {
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
