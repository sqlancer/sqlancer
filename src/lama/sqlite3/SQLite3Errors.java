package lama.sqlite3;

import java.util.Arrays;
import java.util.List;

public class SQLite3Errors {
	
	public static void addDeleteErrors(List<String> errors) {
		// DELETE trigger for a view/table to which colomns were added or deleted
		errors.add("columns but");
		// trigger with on conflict clause
		errors.add("ON CONFLICT clause does not match any PRIMARY KEY or UNIQUE constraint");
	}
	
	
	public static void addExpectedExpressionErrors(List<String> errors) {
		errors.add("FTS expression tree is too large");
		errors.add("[SQLITE_ERROR] SQL error or missing database (integer overflow)");
		errors.add("no such function: json");
		errors.add("second argument to likelihood() must be a constant between 0.0 and 1.0");
		errors.add("ORDER BY term out of range");
		errors.add("GROUP BY term out of range");
		errors.add("not authorized"); // load_extension
		errors.add("aggregate functions are not allowed in the GROUP BY clause");
		errors.add("parser stack overflow");
	}
	
	public static void addMatchQueryErrors(List<String> errors) {
		errors.add("unable to use function MATCH in the requested context");
		errors.add("malformed MATCH expression");
		errors.add("fts5: syntax error near");
		errors.add("no such column"); // vt0.c0 MATCH '-799256540'
		errors.add("unknown special query"); // vt0.c1 MATCH '*YD)LC3^cG|'
		errors.add("fts5: column queries are not supported"); // vt0.c0 MATCH '2016456922'
		errors.add("fts5: phrase queries are not supported");
		errors.add("unterminated string");
	}
	

	public static void addTableManipulationErrors(List<String> errors) {
		errors.add("non-deterministic functions prohibited in CHECK constraints");
		errors.addAll(Arrays.asList("subqueries prohibited in CHECK constraints", "generated columns cannot be part of the PRIMARY KEY", "must have at least one non-generated column"));
	}
	
}
