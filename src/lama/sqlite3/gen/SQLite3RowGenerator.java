package lama.sqlite3.gen;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import lama.Main.StateToReproduce;
import lama.Query;
import lama.QueryAdapter;
import lama.Randomly;
import lama.sqlite3.SQLite3Visitor;
import lama.sqlite3.ast.SQLite3Constant;
import lama.sqlite3.ast.SQLite3Expression;
import lama.sqlite3.schema.SQLite3Schema.Column;
import lama.sqlite3.schema.SQLite3Schema.Table;

public class SQLite3RowGenerator {

	private final Randomly r;
	private Connection con;
	private StateToReproduce state;
	
	private boolean isInsertOrRollback;

	public SQLite3RowGenerator(Randomly r, Connection con, StateToReproduce state) {
		this.r = r;
		this.con = con;
		this.state = state;
	}

	public static Query insertRow(Table table, Connection con, StateToReproduce state, Randomly r) throws SQLException {
		SQLite3RowGenerator generator = new SQLite3RowGenerator(r, con, state);
		String query = generator.insertRow(table);
		return new QueryAdapter(query) {
			public void execute(Connection con) throws SQLException {
				try {
					super.execute(con);
				} catch (SQLException e) {
					if (generator.mightFail && e.getMessage().startsWith("[SQLITE_CONSTRAINT]")) {
						return;
					} else if (e.getMessage().startsWith("[SQLITE_FULL]")) {
						return;
					} else if (e.getMessage()
							.startsWith("[SQLITE_ERROR] SQL error or missing database (integer overflow)")) {
						return;
					} else if (e.getMessage()
							.startsWith("[SQLITE_ERROR] SQL error or missing database (foreign key mismatch")) {
						return;
					} else if (e.getMessage().startsWith(
							"[SQLITE_CONSTRAINT]  Abort due to constraint violation (FOREIGN KEY constraint failed)")) {
						return;
					} else {
						throw e;
					}
				}
			};
			
			@Override
			public boolean couldAffectSchema() {
				return true; // a column can have an ON INSERT clause
//				return generator.isInsertOrRollback;
			}
		};
	}

	private boolean mightFail;

	private String insertRow(Table table) {
		StringBuilder sb = new StringBuilder();
		sb.append("INSERT ");
		if (Randomly.getBoolean()) {
			sb.append("OR IGNORE "); // TODO: try to generate REPLACE
		} else {
			mightFail = true;
			String fromOptions = Randomly.fromOptions("OR REPLACE ", "OR ABORT ", "OR FAIL ", "OR ROLLBACK ");
			if (fromOptions.contentEquals("OR ROLLBACK ")) {
				isInsertOrRollback = true;
			}
			sb.append(fromOptions);
		}
		sb.append("INTO " + table.getName());
		if (Randomly.getBooleanWithSmallProbability()) {
			sb.append(" DEFAULT VALUES");
		} else {
			List<Column> columns = table.getRandomNonEmptyColumnSubset();
			if (columns.size() != table.getColumns().size() || Randomly.getBoolean()) {
				sb.append("(");
				appendColumnNames(columns, sb);
				sb.append(")");
			} else {
				// If the column-name list after table-name is omitted then the number of values
				// inserted into each row must be the same as the number of columns in the
				// table.
				columns = table.getColumns(); // get them again in sorted order
				assert columns.size() == table.getColumns().size();
			}
			sb.append(" VALUES ");
			int nrRows = 1 + Randomly.smallNumber();
			appendNrValues(sb, columns, nrRows);
		}
		sb.append(";");
		return sb.toString();
	}

	private void appendNrValues(StringBuilder sb, List<Column> columns, int nrValues) {
		for (int i = 0; i < nrValues; i++) {
			if (i != 0) {
				sb.append(", ");
			}
			sb.append("(");
			appendValue(sb, columns);
			sb.append(")");
		}
	}

	private void appendValue(StringBuilder sb, List<Column> columns) {
		for (int i = 0; i < columns.size(); i++) {
			if (i != 0) {
				sb.append(", ");
			}
			SQLite3Expression literal;
			if (columns.get(i).isIntegerPrimaryKey()) {
				literal = SQLite3Constant.createIntConstant(r.getInteger());
			} else {
//				literal = SQLite3ExpressionGenerator.getRandomExpression(new ArrayList<>(), false);
				literal = SQLite3ExpressionGenerator.getRandomLiteralValue(false, r);
			}
			SQLite3Visitor visitor = new SQLite3Visitor();
			visitor.visit(literal);
			sb.append(visitor.get());
		}
	}

	private static List<Column> appendColumnNames(List<Column> columns, StringBuilder sb) {
		for (int i = 0; i < columns.size(); i++) {
			if (i != 0) {
				sb.append(", ");
			}
			sb.append(columns.get(i).getName());
		}
		return columns;
	}

}
