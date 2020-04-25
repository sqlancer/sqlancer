package sqlancer.duckdb;

import java.util.Set;

public class DuckDBErrors {

	public static void addExpressionErrors(Set<String> errors) {
		errors.add("Could not convert string");
		errors.add("ORDER term out of range - should be between ");
		errors.add("You might need to add explicit type casts.");
		errors.add("can't be cast because the value is out of range for the destination type");
		errors.add("field value out of range");
		errors.add("Not implemented: Unimplemented type for cast");
		

		errors.add("Type mismatch when combining rows"); // BETWEEN
		
		errors.add("invalid UTF-8"); // TODO
		errors.add("String value is not valid UTF8");
		
		errors.add("Conversion: Invalid TypeId "); // TODO
		
		errors.add("GROUP BY clause cannot contain aggregates!"); // investigate
		
		addRegexErrors(errors);
		
		addFunctionErrors(errors);
		
		errors.add("Overflow in multiplication");
		errors.add("Out of Range");
	}

	private static void addRegexErrors(Set<String> errors) {
		errors.add("missing ]");
		errors.add("missing )");
		errors.add("invalid escape sequence");
		errors.add("no argument for repetition operator: ");
		errors.add("bad repetition operator");
		errors.add("trailing \\");
		errors.add("invalid perl operator");
		errors.add("invalid character class range");
		errors.add("width is not integer");
	}

	private static void addFunctionErrors(Set<String> errors) {
		errors.add("SUBSTRING cannot handle negative offsets");
		errors.add("is undefined outside [-1,1]"); // ACOS etc
		errors.add("invalid type specifier"); // PRINTF
		errors.add("argument index out of range"); // PRINTF
		errors.add("invalid format string"); // PRINTF
	}

	public static void addInsertErrors(Set<String> errors) {
		errors.add("NOT NULL constraint failed");
		errors.add("PRIMARY KEY or UNIQUE constraint violated");
		errors.add("duplicate key value violates primary key or unique constraint");
		errors.add("can't be cast because the value is out of range for the destination type");
		errors.add("Could not convert string");
		errors.add("timestamp field value out of range");
		errors.add("Not implemented: Unimplemented type for cast"); // TODO: report?
		errors.add("date/time field value out of range");
		errors.add("CHECK constraint failed");
	}

	public static void addGroupByErrors(Set<String> errors) {
		errors.add("must appear in the GROUP BY clause or be used in an aggregate function");
		errors.add("GROUP BY term out of range");		
	}

}
