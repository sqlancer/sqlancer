package sqlancer.tidb;

import java.util.Set;

public class TiDBErrors {

	public static void addExpressionErrors(Set<String> errors) {
		errors.add("error parsing regexp");
		errors.add("BIGINT UNSIGNED value is out of range");
		errors.add("overflows bigint");
		errors.add("strconv.ParseFloat: parsing");
		errors.add("in 'order clause'"); // int constants in order by clause
		
		// functions
		errors.add("BIGINT value is out of range");
		
		// https://github.com/pingcap/tidb/issues/15790
		errors.add("Data truncation: %s value is out of range in '%s'");
	}

	public static void addExpressionHavingErrors(Set<String> errors) {
		errors.add("is not in GROUP BY clause and contains nonaggregated column");
		errors.add("Unknown column");
	}

}
