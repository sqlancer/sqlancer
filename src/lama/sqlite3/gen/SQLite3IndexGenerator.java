package lama.sqlite3.gen;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import lama.Query;
import lama.QueryAdapter;
import lama.Randomly;
import lama.StateToReproduce;
import lama.sqlite3.SQLite3ToStringVisitor;
import lama.sqlite3.ast.SQLite3Expression;
import lama.sqlite3.schema.SQLite3Schema;
import lama.sqlite3.schema.SQLite3Schema.Column;
import lama.sqlite3.schema.SQLite3Schema.Table;

// see https://www.sqlite.org/lang_createindex.html
public class SQLite3IndexGenerator {

	public static Query insertIndex(Connection con, StateToReproduce state, Randomly r) throws SQLException {
		return new SQLite3IndexGenerator(con, state, r).query;
	}

	// FIXME: always i0?;
	private int indexNr;
	private final Query query;
	boolean isUnique;
	private final Randomly r;
	private final List<String> errors = new ArrayList<>();

	public SQLite3IndexGenerator(Connection con, StateToReproduce state, Randomly r) throws SQLException {
		this.r = r;
		SQLite3Schema s = SQLite3Schema.fromConnection(con);
		Table t = s.getRandomTableNoView();
		String q = createIndex(t, t.getColumns());
		if (isUnique) {
			errors.add("[SQLITE_CONSTRAINT]  Abort due to constraint violation (UNIQUE constraint failed");
		}
		errors.add("[SQLITE_ERROR] SQL error or missing database (integer overflow)");
		errors.add("[SQLITE_ERROR] SQL error or missing database (parser stack overflow)");

		/**
		 * Strings in single quotes are sometimes interpreted as column names. Since we
		 * found an issue with double quotes, they can no longer be used (see
		 * https://sqlite.org/src/info/9b78184b). Single quotes are interpreted as
		 * column names in certain contexts (see
		 * https://www.mail-archive.com/sqlite-users@mailinglists.sqlite.org/msg115014.html).
		 */
		errors.add("[SQLITE_ERROR] SQL error or missing database (no such column:");
		errors.add(
				"[SQLITE_ERROR] SQL error or missing database (second argument to likelihood() must be a constant between 0.0 and 1.0)");
		query = new QueryAdapter(q, errors);
	}

	private String createIndex(Table t, List<Column> columns) {
		StringBuilder sb = new StringBuilder();
		sb.append("CREATE");
		if (Randomly.getBoolean()) {
			isUnique = true;
			sb.append(" UNIQUE");
		}
		sb.append(" INDEX");
		sb.append(" IF NOT EXISTS");
		sb.append(" " + getIndexName());
		sb.append(" ON");
		sb.append(" " + t.getName());
		sb.append("(");
		for (int i = 0; i < columns.size(); i++) {
			if (i != 0) {
				sb.append(",");
			}
			SQLite3Expression expr = new SQLite3ExpressionGenerator().getRandomExpression(columns, true, r);
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
			SQLite3Expression expr = new SQLite3ExpressionGenerator().getRandomExpression(columns, true, r);
			SQLite3ToStringVisitor visitor = new SQLite3ToStringVisitor();
			visitor.fullyQualifiedNames = false;
			visitor.visit(expr);
			sb.append(visitor.get());
		}
		return sb.toString();
	}

	enum ExpressionType {
		COLUMN_NAME, LITERAL_VALUE, UNARY_OPERATOR
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

	private String getIndexName() {
		return SQLite3Common.createIndexName(r.getInteger(0, 10));
	}

}
