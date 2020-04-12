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
		
		if (true) {
			// https://github.com/cwida/duckdb/issues/513
			errors.add("Not implemented: Unimplemented type for nested loop join!");
		}
		
		errors.add("Type mismatch when combining rows"); // BETWEEN
		
		addRegexErrors(errors);
		
		addFunctionErrors(errors);
	}

	private static void addRegexErrors(Set<String> errors) {
		errors.add("missing ]");
		errors.add("missing )");
		errors.add("invalid escape sequence");
		errors.add("no argument for repetition operator: ");
		errors.add("bad repetition operator");
		errors.add("trailing \\");
		errors.add("invalid perl operator");
	}

	private static void addFunctionErrors(Set<String> errors) {
		errors.add("SUBSTRING cannot handle negative offsets");
		errors.add("is undefined outside [-1,1]"); // ACOS etc
	}

}
