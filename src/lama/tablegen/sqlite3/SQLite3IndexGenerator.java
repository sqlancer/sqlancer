package lama.tablegen.sqlite3;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import lama.Expression;
import lama.Main.StateToReproduce;
import lama.Randomly;
import lama.schema.Schema;
import lama.schema.Schema.Column;
import lama.schema.Schema.Table;
import lama.sqlite3.SQLite3Visitor;

// see https://www.sqlite.org/lang_createindex.html
public class SQLite3IndexGenerator {

	public static void insertIndex(Connection con, StateToReproduce state) throws SQLException {
		new SQLite3IndexGenerator(con, state);
	}

	private int indexNr;

	public SQLite3IndexGenerator(Connection con, StateToReproduce state) throws SQLException {
		Schema s = Schema.fromConnection(con);
		Table t = s.getRandomTable();
		String query = createIndex(t, t.getColumns());
		try (Statement stm = con.createStatement()) {
			state.statements.add(query);
			stm.execute(query);
		}
	}

	private String createIndex(Table t, List<Column> columns) {
		StringBuilder sb = new StringBuilder();
		sb.append("CREATE");
		if (Randomly.getBoolean()) {
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
			Expression expr = SQLite3ExpressionGenerator.getRandomExpression(columns, true);
			SQLite3Visitor visitor = new SQLite3Visitor();
			visitor.setStringsAsDoubleQuotes(true);
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
			Expression expr = SQLite3ExpressionGenerator.getRandomExpression(columns, true);
			SQLite3Visitor visitor = new SQLite3Visitor();
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
		return SQLite3Common.createIndexName(indexNr++);
	}


}
