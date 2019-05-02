package lama.tablegen.sqlite3;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import lama.Expression;
import lama.Main;
import lama.Randomly;
import lama.Main.StateToReproduce;
import lama.schema.Schema.Column;
import lama.schema.Schema.Table;
import lama.sqlite3.SQLite3Visitor;

public class Sqlite3RowGenerator {

	public static void insertRows(Table table, Connection con, StateToReproduce state) throws SQLException {
		for (int i = 0; i < Main.NR_INSERT_ROW_TRIES; i++) {
			String query = insertRow(table);
			try (Statement s = con.createStatement()) {
				state.statements.add(query);
				s.execute(query);
			} catch (Exception e) {
				e.printStackTrace();
				System.err.println(state);
			}
		}
	}

	private static String insertRow(Table table) {
		StringBuilder sb = new StringBuilder();
		sb.append("INSERT OR IGNORE INTO " + table.getName());
		if (Randomly.getBooleanWithSmallProbability()) {
			sb.append(" DEFAULT VALUES");
		} else {
			sb.append("(");
			List<Column> columns = table.getColumns();
			for (int i = 0; i < columns.size(); i++) {
				if (i != 0) {
					sb.append(", ");
				}
				sb.append(columns.get(i).getName());
			}
			sb.append(") VALUES (");
			for (int i = 0; i < columns.size(); i++) {
				if (i != 0) {
					sb.append(", ");
				}
				Expression randomLiteral = SQLite3ExpressionGenerator.getRandomLiteralValue(false);
				SQLite3Visitor visitor = new SQLite3Visitor();
				visitor.visit(randomLiteral);
				sb.append(visitor.get());
			}
			sb.append(")");
		}
		return sb.toString();
	}

}
