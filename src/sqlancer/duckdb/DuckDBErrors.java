package sqlancer.duckdb;

import java.util.Set;

public class DuckDBErrors {

	public static void addExpressionErrors(Set<String> errors) {
		errors.add("Could not convert string");
		errors.add("ORDER term out of range - should be between ");
															// there are more results that can be fetched, even if the
															// result set is empty
		errors.add("You might need to add explicit type casts.");
		errors.add("can't be cast because the value is out of range for the destination type");
		errors.add("Invalid TypeId -1"); // TODO report
		
		errors.add("missing ]");
		errors.add("missing )");
		errors.add("invalid escape sequence");
		errors.add("no argument for repetition operator: ");
		errors.add("bad repetition operator");
		
		if (true) {
			// https://github.com/cwida/duckdb/issues/503
			errors.add("Unhandled type for empty NL join");
			errors.add("Not implemented: Unimplemented type for nested loop join!");
		}
		
		if (true) {
			// 
			errors.add("INTERNAL: Failed to bind column reference");
		}
		
		addFunctionErrors(errors);
	}

	private static void addFunctionErrors(Set<String> errors) {
		errors.add("SUBSTRING cannot handle negative offsets");
		errors.add("is undefined outside [-1,1]"); // ACOS etc
	}

}
