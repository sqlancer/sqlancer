package lama.sqlite3.gen.dml;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import lama.Query;
import lama.QueryAdapter;
import lama.Randomly;
import lama.sqlite3.SQLite3Errors;
import lama.sqlite3.SQLite3Provider.SQLite3GlobalState;
import lama.sqlite3.SQLite3ToStringVisitor;
import lama.sqlite3.SQLite3Visitor;
import lama.sqlite3.ast.SQLite3Constant;
import lama.sqlite3.ast.SQLite3Expression;
import lama.sqlite3.gen.SQLite3ExpressionGenerator;
import lama.sqlite3.schema.SQLite3Schema.SQLite3Column;
import lama.sqlite3.schema.SQLite3Schema.Table;

public class SQLite3InsertGenerator {

	private final Randomly r;
	private final List<String> errors;
	private final SQLite3GlobalState globalState;

	public SQLite3InsertGenerator(SQLite3GlobalState globalState, Randomly r, Connection con) {
		this.globalState = globalState;
		this.r = r;
		errors = new ArrayList<>();
	}

	public static Query insertRow(SQLite3GlobalState globalState) throws SQLException {
		Table randomTable = globalState.getSchema().getRandomTableOrBailout(t -> !t.isView() && !t.isReadOnly());
		return insertRow(globalState, randomTable);
	}

	public static Query insertRow(SQLite3GlobalState globalState, Table randomTable) {
		SQLite3InsertGenerator generator = new SQLite3InsertGenerator(globalState, globalState.getRandomly(), globalState.getConnection());
		String query = generator.insertRow(randomTable);
		return new QueryAdapter(query, generator.errors, true);
	}

	private String insertRow(Table table) {
		errors.add("cannot UPDATE generated column");
		errors.add("[SQLITE_CONSTRAINT]");
		errors.add("[SQLITE_FULL]");
		errors.add("[SQLITE_ERROR] SQL error or missing database (foreign key mismatch");
		errors.add("[SQLITE_CONSTRAINT]  Abort due to constraint violation (FOREIGN KEY constraint failed)");
		// // TODO: also check if the table is really missing (caused by a DROP TABLE)
		errors.add("[SQLITE_ERROR] SQL error or missing database (no such table:");
		errors.add("ON CONFLICT clause does not match any PRIMARY KEY or UNIQUE constraint"); // trigger
		errors.add("no such column"); // trigger
		errors.add("values were supplied"); // trigger
		errors.add("Data type mismatch (datatype mismatch)"); // trigger
		errors.add("too many levels of trigger recursion");
		errors.add("String or BLOB exceeds size limit");

		errors.add("A table in the database is locked");
		errors.add("cannot INSERT into generated column"); // TODO: filter out generated columns
		errors.add("A table in the database is locked");
		SQLite3Errors.addInsertNowErrors(errors);
		SQLite3Errors.addExpectedExpressionErrors(errors);
		StringBuilder sb = new StringBuilder();
		sb.append("INSERT ");
		if (Randomly.getBoolean()) {
			sb.append("OR IGNORE "); // TODO: try to generate REPLACE
		} else {
			String fromOptions = Randomly.fromOptions("OR REPLACE ", "OR ABORT ", "OR FAIL ", "OR ROLLBACK ");
			sb.append(fromOptions);
		}
		boolean defaultValues = false;
		sb.append("INTO " + table.getName());
		if (Randomly.getBooleanWithSmallProbability()) {
			defaultValues = true;
			sb.append(" DEFAULT VALUES");
		} else {
			List<SQLite3Column> columns = table.getRandomNonEmptyColumnSubset();
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
		boolean columnsInConflictClause = Randomly.getBoolean();
		if (!defaultValues && Randomly.getBooleanWithSmallProbability() && !table.isVirtual()) {
			sb.append(" ON CONFLICT");
			if (columnsInConflictClause) {
				sb.append("(");
				sb.append(table.getRandomNonEmptyColumnSubset().stream().map(c -> c.getName())
						.collect(Collectors.joining(", ")));
				sb.append(")");
				errors.add("ON CONFLICT clause does not match any PRIMARY KEY or UNIQUE constraint");
			}
			sb.append(" DO ");
			if (Randomly.getBoolean() || !columnsInConflictClause) {
				sb.append("NOTHING");
			} else {
				sb.append("UPDATE SET ");
				List<SQLite3Column> columns = table.getRandomNonEmptyColumnSubset();
				for (int i = 0; i < columns.size(); i++) {
					if (i != 0) {
						sb.append(", ");
					}
					sb.append(columns.get(i).getName());
					sb.append("=");
					if (Randomly.getBoolean()) {
						sb.append(SQLite3Visitor.asString(SQLite3ExpressionGenerator.getRandomLiteralValue(globalState)));
					} else {
						if (Randomly.getBoolean()) {
							sb.append("excluded.");
						}
						sb.append(table.getRandomColumn().getName());
					}

				}
				errors.add("Abort due to constraint violation");
				errors.add("Data type mismatch (datatype mismatch)");
				if (Randomly.getBoolean()) {
					sb.append(" WHERE ");
					sb.append(SQLite3Visitor.asString(
							new SQLite3ExpressionGenerator(globalState).setColumns(table.getColumns()).getRandomExpression()));
				}
			}
		}
		return sb.toString();
	}

	private void appendNrValues(StringBuilder sb, List<SQLite3Column> columns, int nrValues) {
		for (int i = 0; i < nrValues; i++) {
			if (i != 0) {
				sb.append(", ");
			}
			sb.append("(");
			appendValue(sb, columns);
			sb.append(")");
		}
	}

	private void appendValue(StringBuilder sb, List<SQLite3Column> columns) {
		for (int i = 0; i < columns.size(); i++) {
			if (i != 0) {
				sb.append(", ");
			}
			SQLite3Expression literal;
			if (columns.get(i).isIntegerPrimaryKey()) {
				literal = SQLite3Constant.createIntConstant(r.getInteger());
			} else {
				if (Randomly.getBooleanWithSmallProbability()) {
					literal = new SQLite3ExpressionGenerator(globalState).getRandomExpression();
				} else {
					literal = SQLite3ExpressionGenerator.getRandomLiteralValue(globalState);
				}
			}
			SQLite3ToStringVisitor visitor = new SQLite3ToStringVisitor();
			visitor.visit(literal);
			sb.append(visitor.get());
		}
	}

	private static List<SQLite3Column> appendColumnNames(List<SQLite3Column> columns, StringBuilder sb) {
		for (int i = 0; i < columns.size(); i++) {
			if (i != 0) {
				sb.append(", ");
			}
			sb.append(columns.get(i).getName());
		}
		return columns;
	}

}
