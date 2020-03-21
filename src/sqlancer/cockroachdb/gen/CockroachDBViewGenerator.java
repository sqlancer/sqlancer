package sqlancer.cockroachdb.gen;

import java.util.HashSet;
import java.util.Set;

import sqlancer.Query;
import sqlancer.QueryAdapter;
import sqlancer.Randomly;
import sqlancer.cockroachdb.CockroachDBErrors;
import sqlancer.cockroachdb.CockroachDBProvider.CockroachDBGlobalState;

public class CockroachDBViewGenerator {

	public static Query generate(CockroachDBGlobalState globalState) {
		int nrColumns = 1;
		StringBuilder sb = new StringBuilder("CREATE ");
		if (Randomly.getBoolean() && false) {
			sb.append("TEMP ");
		}
		sb.append("VIEW " + Randomly.fromOptions("v0", "v1", "v1"));
		sb.append("(");
		for (int i = 0; i < nrColumns; i++) {
			if (i != 0) {
				sb.append(", ");
			}
			sb.append("c" + i);
		}
		sb.append(") AS ");
		sb.append(CockroachDBRandomQuerySynthesizer.generate(globalState, nrColumns).getQueryString());
		Set<String> errors = new HashSet<>();
		CockroachDBErrors.addExpressionErrors(errors);
		CockroachDBErrors.addTransactionErrors(errors);
		errors.add("value type unknown cannot be used for table columns");
		errors.add("already exists");
		return new QueryAdapter(sb.toString(), errors, true);
	}
	
}
