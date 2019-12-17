package lama.sqlite3.dml;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import lama.Query;
import lama.QueryAdapter;
import lama.Randomly;
import lama.sqlite3.SQLite3Errors;
import lama.sqlite3.SQLite3Provider.SQLite3GlobalState;
import lama.sqlite3.SQLite3Visitor;
import lama.sqlite3.gen.SQLite3ExpressionGenerator;
import lama.sqlite3.schema.SQLite3Schema.Table;

public class SQLite3DeleteGenerator {

	public static Query deleteContent(SQLite3GlobalState globalState) {
		Table tableName = globalState.getSchema().getRandomTableNoViewOrBailout();
		return deleteContent(globalState, tableName);
	}

	public static Query deleteContent(SQLite3GlobalState globalState, Table tableName) {
		StringBuilder sb = new StringBuilder();
		sb.append("DELETE FROM ");
		sb.append(tableName.getName());
		if (Randomly.getBoolean()) {
			sb.append(" WHERE ");
			sb.append(SQLite3Visitor.asString(new SQLite3ExpressionGenerator(globalState.getRandomly())
					.setColumns(tableName.getColumns()).getRandomExpression()));
		}
		List<String> errors = new ArrayList<>();
		SQLite3Errors.addExpectedExpressionErrors(errors);
		errors.addAll(Arrays.asList("[SQLITE_ERROR] SQL error or missing database (foreign key mismatch",
				"[SQLITE_CONSTRAINT]  Abort due to constraint violation ",
				"[SQLITE_ERROR] SQL error or missing database (parser stack overflow)",
				"[SQLITE_ERROR] SQL error or missing database (no such table:", "no such column",
				"too many levels of trigger recursion", "cannot UPDATE generated column",
				"cannot INSERT into generated column"));
		SQLite3Errors.addDeleteErrors(errors);
		return new QueryAdapter(sb.toString(), errors, true);
	}

}
