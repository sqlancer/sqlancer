package lama.sqlite3.gen.ddl;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import lama.Query;
import lama.QueryAdapter;
import lama.Randomly;
import lama.sqlite3.SQLite3Errors;
import lama.sqlite3.SQLite3Provider;
import lama.sqlite3.SQLite3Provider.SQLite3GlobalState;
import lama.sqlite3.SQLite3ToStringVisitor;
import lama.sqlite3.ast.SQLite3Expression;
import lama.sqlite3.gen.SQLite3Common;
import lama.sqlite3.gen.SQLite3ExpressionGenerator;
import lama.sqlite3.schema.SQLite3Schema.Column;
import lama.sqlite3.schema.SQLite3Schema.Table;

// see https://www.sqlite.org/lang_createindex.html
public class SQLite3IndexGenerator {

	public static Query insertIndex(SQLite3GlobalState globalState) throws SQLException {
		return new SQLite3IndexGenerator(globalState).create();
	}

	private final List<String> errors = new ArrayList<>();
	private final SQLite3GlobalState globalState;

	public SQLite3IndexGenerator(SQLite3GlobalState globalState) throws SQLException {
		this.globalState = globalState;
	}

	private Query create() throws SQLException {
		Table t = globalState.getSchema().getRandomTableOrBailout(tab -> !tab.isView() && !tab.isVirtual());
		String q = createIndex(t, t.getColumns());
		errors.add("[SQLITE_ERROR] SQL error or missing database (parser stack overflow)");
		errors.add("subqueries prohibited in index expressions");
		errors.add("subqueries prohibited in partial index WHERE clauses");
		errors.add("non-deterministic use of time() in an index");
		errors.add("non-deterministic use of strftime() in an index");
		SQLite3Errors.addExpectedExpressionErrors(errors);
		if (!SQLite3Provider.MUST_KNOW_RESULT) {
			// can only happen when PRAGMA case_sensitive_like=ON;
			errors.add("non-deterministic functions prohibited");
		}

		/**
		 * Strings in single quotes are sometimes interpreted as column names. Since we
		 * found an issue with double quotes, they can no longer be used (see
		 * https://sqlite.org/src/info/9b78184b). Single quotes are interpreted as
		 * column names in certain contexts (see
		 * https://www.mail-archive.com/sqlite-users@mailinglists.sqlite.org/msg115014.html).
		 */
		errors.add("[SQLITE_ERROR] SQL error or missing database (no such column:");
		return new QueryAdapter(q, errors, true);
	}

	private String createIndex(Table t, List<Column> columns) {
		StringBuilder sb = new StringBuilder();
		sb.append("CREATE");
		if (Randomly.getBoolean()) {
			errors.add("[SQLITE_CONSTRAINT]  Abort due to constraint violation (UNIQUE constraint failed");
			sb.append(" UNIQUE");
		}
		sb.append(" INDEX");
		if (Randomly.getBoolean()) {
			sb.append(" IF NOT EXISTS");
		}
		sb.append(" " + SQLite3Common.getFreeIndexName(globalState.getSchema()));
		sb.append(" ON");
		sb.append(" " + t.getName());
		sb.append("(");
		for (int i = 0; i < columns.size(); i++) {
			if (i != 0) {
				sb.append(",");
			}
			SQLite3Expression expr = new SQLite3ExpressionGenerator(globalState).expectedErrors(errors).setColumns(columns).deterministicOnly().getRandomExpression();
			SQLite3ToStringVisitor visitor = new SQLite3ToStringVisitor();
			visitor.fullyQualifiedNames = false;
			visitor.visit(expr);
			sb.append(visitor.get());
			if (Randomly.getBoolean()) {
				sb.append(SQLite3Common.getRandomCollate());
			}
			appendPotentialOrdering(sb);
		}
		sb.append(")");
		if (Randomly.getBoolean()) {
			sb.append(" WHERE ");
			SQLite3Expression expr = new SQLite3ExpressionGenerator(globalState).setColumns(columns).deterministicOnly().getRandomExpression();
			SQLite3ToStringVisitor visitor = new SQLite3ToStringVisitor();
			visitor.fullyQualifiedNames = false;
			visitor.visit(expr);
			sb.append(visitor.get());
		}
		return sb.toString();
	}


	/**
	 * Appends ASC, DESC, or nothing
	 */
	private void appendPotentialOrdering(StringBuilder sb) {
		if (Randomly.getBoolean()) {
			if (Randomly.getBoolean()) {
				sb.append(" ASC");
			} else {
				sb.append(" DESC");
			}
		}
	}


}
