package lama.sqlite3.gen;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import lama.Main.StateToReproduce;
import lama.Query;
import lama.QueryAdapter;
import lama.Randomly;
import lama.sqlite3.SQLite3Visitor;
import lama.sqlite3.ast.SQLite3Constant;
import lama.sqlite3.schema.SQLite3Schema.Column;
import lama.sqlite3.schema.SQLite3Schema.Table;

public class SQLite3UpdateGenerator {

	private final StringBuilder sb = new StringBuilder();
	private boolean mightFail;
	private final Randomly r;
	private boolean mightRollback;

	public SQLite3UpdateGenerator(Randomly r) {
		this.r = r;
	}

	public static Query updateRow(Table table, Connection con, StateToReproduce state, Randomly r) {
		SQLite3UpdateGenerator generator = new SQLite3UpdateGenerator(r);
		return new QueryAdapter(generator.update(table, con, state)) {
			public void execute(Connection con) throws SQLException {
				try {
					super.execute(con);
				} catch (SQLException e) {
					if (generator.mightFail && e.getMessage().startsWith("[SQLITE_CONSTRAINT]")) {
						return;
					} else if (e.getMessage()
							.startsWith("[SQLITE_ERROR] SQL error or missing database (integer overflow)")) {
						return;
					} else if (e.getMessage()
							.startsWith("[SQLITE_ERROR] SQL error or missing database (foreign key mismatch")) {
						return;
					} else if (e.getMessage().startsWith("[SQLITE_CONSTRAINT]  Abort due to constraint violation")) {
						return;
					} else if (e.getMessage()
							.startsWith("[SQLITE_ERROR] SQL error or missing database (parser stack overflow)")) {
						return;
					} else if (e.getMessage().startsWith(
							"[SQLITE_ERROR] SQL error or missing database (second argument to likelihood() must be a constant between 0.0 and 1.0)")) {
						return;
					}

					else {
						throw e;
					}
				}
			};

			@Override
			public boolean couldAffectSchema() {
				// column could have an ON UPDATE clause
				return true; // generator.mightRollback;
			}
		};
	}

	private String update(Table table, Connection con, StateToReproduce state) {
		sb.append("UPDATE ");
		if (Randomly.getBoolean()) {
			sb.append("OR IGNORE ");
		} else {
			mightFail = true;
			if (Randomly.getBoolean()) {
				String fromOptions = Randomly.fromOptions("OR ROLLBACK", "OR ABORT", "OR REPLACE", "OR FAIL");
				if (fromOptions.contentEquals("OR ROLLBACK")) {
					mightRollback = true;
				}
				sb.append(fromOptions);
				sb.append(" ");
				mightFail = true;
			}
		}
// TODO Beginning in SQLite version 3.15.0 (2016-10-14), an assignment in the SET clause can be a parenthesized list of column names on the left and a row value of the same size on the right.

		sb.append(table.getName());
		sb.append(" SET ");
		List<Column> columnsToUpdate = Randomly.nonEmptySubset(table.getColumns());
		for (int i = 0; i < columnsToUpdate.size(); i++) {
			if (i != 0) {
				sb.append(", ");
			}
			sb.append(columnsToUpdate.get(i).getName());
			sb.append(" = ");
			if (columnsToUpdate.get(i).isIntegerPrimaryKey()) {
				sb.append(SQLite3Visitor.asString(SQLite3Constant.createIntConstant(r.getInteger())));
			} else {
				sb.append(SQLite3Visitor.asString(SQLite3ExpressionGenerator.getRandomLiteralValue(false, r)));
			}
		}

		if (Randomly.getBoolean()) {
			sb.append(" WHERE ");
			String whereClause = SQLite3Visitor
					.asString(new SQLite3ExpressionGenerator().getRandomExpression(table.getColumns(), false, r));
			sb.append(whereClause);
		}

		// ORDER BY and LIMIT are only supported by enabling a compile-time option
//		List<Expression> expressions = QueryGenerator.generateOrderBy(table.getColumns());
//		if (!expressions.isEmpty()) {
//			sb.append(" ORDER BY ");
//			sb.append(expressions.stream().map(e -> SQLite3Visitor.asString(e)).collect(Collectors.joining(", ")));
//		}

		return sb.toString();

	}

}
