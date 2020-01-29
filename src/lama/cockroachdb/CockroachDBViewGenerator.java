package lama.cockroachdb;

import java.util.HashSet;
import java.util.Set;

import lama.Query;
import lama.QueryAdapter;
import lama.cockroachdb.CockroachDBProvider.CockroachDBGlobalState;

public class CockroachDBViewGenerator {

	public static Query generate(CockroachDBGlobalState globalState) {
		int nrColumns = 1;
		StringBuilder sb = new StringBuilder("CREATE VIEW v0");
		
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
		return new QueryAdapter(sb.toString(), errors, true);
	}
	
}
