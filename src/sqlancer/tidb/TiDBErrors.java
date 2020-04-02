package sqlancer.tidb;

import java.util.Set;

public class TiDBErrors {

	public static void addExpressionErrors(Set<String> errors) {
		errors.add("DECIMAL value is out of range");
		errors.add("error parsing regexp");
		errors.add("BIGINT UNSIGNED value is out of range");
		errors.add("overflows bigint");
		errors.add("strconv.ParseFloat: parsing");
		errors.add("in 'order clause'"); // int constants in order by clause
		
		// functions
		errors.add("BIGINT value is out of range");
		
		// https://github.com/pingcap/tidb/issues/15790
		errors.add("Data truncation: %s value is out of range in '%s'");
		
		// known issue: https://github.com/pingcap/tidb/issues/14819
		errors.add("Wrong plan type for dataReaderBuilder");
		
		if (true) {
			// https://github.com/pingcap/tidb/issues/16017
			errors.add("Can't find a proper physical plan for this query");
		}
		
		
		errors.add("DOUBLE value is out of range in 'cot(0)'");
		errors.add("DOUBLE value is out of range in 'pow");
		errors.add("DOUBLE value is out of range in 'exp(");
		
		errors.add("index out of range"); // https://github.com/pingcap/tidb/issues/15810
		errors.add("baseBuiltinFunc.evalString() should never be called, please contact the TiDB team for help"); // https://github.com/pingcap/tidb/issues/15847
		errors.add("unsupport column type for encode 6"); // https://github.com/pingcap/tidb/issues/15850
	}

	public static void addExpressionHavingErrors(Set<String> errors) {
		errors.add("is not in GROUP BY clause and contains nonaggregated column");
		errors.add("Unknown column");
	}

}
