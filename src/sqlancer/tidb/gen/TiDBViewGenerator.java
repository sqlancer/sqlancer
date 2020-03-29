package sqlancer.tidb.gen;

import java.util.HashSet;
import java.util.Set;

import sqlancer.Query;
import sqlancer.QueryAdapter;
import sqlancer.Randomly;
import sqlancer.tidb.TiDBErrors;
import sqlancer.tidb.TiDBProvider.TiDBGlobalState;

public class TiDBViewGenerator {

	public static Query getQuery(TiDBGlobalState globalState) {
		int nrColumns = Randomly.smallNumber() + 1;
		StringBuilder sb = new StringBuilder("CREATE ");
		sb.append("VIEW ");
		sb.append(globalState.getSchema().getFreeViewName());
		sb.append("(");
		for (int i = 0; i < nrColumns; i++) {
			if (i != 0) {
				sb.append(", ");
			}
			sb.append("c" + i);
		}
		sb.append(") AS ");
		sb.append(TiDBRandomQuerySynthesizer.generate(globalState, nrColumns).getQueryString());
		Set<String> errors = new HashSet<>();
		TiDBErrors.addExpressionErrors(errors);
		errors.add("references invalid table(s) or column(s) or function(s) or definer/invoker of view lack rights to use them");
		errors.add("Unknown column ");
		return new QueryAdapter(sb.toString(), errors, true);
	}

}
