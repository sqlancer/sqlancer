package lama.sqlite3.dml;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import lama.Query;
import lama.QueryAdapter;
import lama.Randomly;
import lama.sqlite3.SQLite3Errors;
import lama.sqlite3.SQLite3Visitor;
import lama.sqlite3.ast.SQLite3Constant;
import lama.sqlite3.gen.SQLite3ExpressionGenerator;
import lama.sqlite3.schema.SQLite3Schema.Column;
import lama.sqlite3.schema.SQLite3Schema.Table;

public class SQLite3UpdateGenerator {

	private final StringBuilder sb = new StringBuilder();
	private final Randomly r;
	private final List<String> errors = new ArrayList<>();

	public SQLite3UpdateGenerator(Randomly r) {
		this.r = r;
	}

	public static Query updateRow(Table table, Randomly r) {
		SQLite3UpdateGenerator generator = new SQLite3UpdateGenerator(r);
		return generator.update(table);
	}

	private Query update(Table table) {
		sb.append("UPDATE ");
		if (Randomly.getBoolean()) {
			sb.append("OR IGNORE ");
		} else {
			if (Randomly.getBoolean()) {
				String fromOptions = Randomly.fromOptions("OR ROLLBACK", "OR ABORT", "OR REPLACE", "OR FAIL");
				sb.append(fromOptions);
				sb.append(" ");
			}
			errors.add("[SQLITE_CONSTRAINT]");
		}
		// TODO Beginning in SQLite version 3.15.0 (2016-10-14), an assignment in the SET clause can be a parenthesized list of column names on the left and a row value of the same size on the right.

		sb.append(table.getName());
		sb.append(" SET ");
		List<Column> columnsToUpdate = Randomly.nonEmptySubsetPotentialDuplicates(table.getColumns());
		if (Randomly.getBoolean()) {
			sb.append("(");
			sb.append(columnsToUpdate.stream().map(c -> c.getName()).collect(Collectors.joining(", ")));
			sb.append(")");
			sb.append("=");
			sb.append("(");
			for (int i = 0; i < columnsToUpdate.size(); i++) {
				if (i != 0) {
					sb.append(", ");
				}
				getToUpdateValue(columnsToUpdate, i);
			}
			sb.append(")");
			// row values
		} else {
			for (int i = 0; i < columnsToUpdate.size(); i++) {
				if (i != 0) {
					sb.append(", ");
				}
				sb.append(columnsToUpdate.get(i).getName());
				sb.append(" = ");
				getToUpdateValue(columnsToUpdate, i);
			}
		}

		if (Randomly.getBoolean()) {
			sb.append(" WHERE ");
			String whereClause = SQLite3Visitor.asString(new SQLite3ExpressionGenerator(r).expectedErrors(errors)
					.setColumns(table.getColumns()).getRandomExpression());
			sb.append(whereClause);
		}

		// ORDER BY and LIMIT are only supported by enabling a compile-time option
//		List<Expression> expressions = QueryGenerator.generateOrderBy(table.getColumns());
//		if (!expressions.isEmpty()) {
//			sb.append(" ORDER BY ");
//			sb.append(expressions.stream().map(e -> SQLite3Visitor.asString(e)).collect(Collectors.joining(", ")));
//		}

		errors.add("[SQLITE_ERROR] SQL error or missing database (foreign key mismatch");
		errors.add("[SQLITE_CONSTRAINT]  Abort due to constraint violation");
		errors.add("[SQLITE_ERROR] SQL error or missing database (parser stack overflow)");
		errors.add(
				"[SQLITE_ERROR] SQL error or missing database (second argument to likelihood() must be a constant between 0.0 and 1.0)");
		errors.add("[SQLITE_ERROR] SQL error or missing database (no such table:");
		// for views
		errors.add("ORDER BY term out of range");
		errors.add("no such column");
		errors.add("(too many levels of trigger recursion");
		errors.add("String or BLOB exceeds size limit");
		errors.add("cannot UPDATE generated column");
		errors.add("unknown function: json_type");

		// TODO not update generated columns?
		errors.add("cannot INSERT into generated column");
		SQLite3Errors.addInsertNowErrors(errors);
		SQLite3Errors.addExpectedExpressionErrors(errors);
		SQLite3Errors.addDeleteErrors(errors);
		return new QueryAdapter(sb.toString(), errors, true /* column could have an ON UPDATE clause */);

	}

	private void getToUpdateValue(List<Column> columnsToUpdate, int i) {
		if (columnsToUpdate.get(i).isIntegerPrimaryKey()) {
			sb.append(SQLite3Visitor.asString(SQLite3Constant.createIntConstant(r.getInteger())));
		} else {
			sb.append(SQLite3Visitor.asString(SQLite3ExpressionGenerator.getRandomLiteralValue(r)));
		}
	}

}
