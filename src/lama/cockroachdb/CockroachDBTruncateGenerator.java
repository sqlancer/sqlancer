package lama.cockroachdb;

import java.util.HashSet;
import java.util.Set;

import lama.Query;
import lama.QueryAdapter;
import lama.Randomly;
import lama.cockroachdb.CockroachDBProvider.CockroachDBGlobalState;

public class CockroachDBTruncateGenerator {

	// https://www.cockroachlabs.com/docs/v19.2/truncate.html
	public static Query truncate(CockroachDBGlobalState globalState) {
		Set<String> errors = new HashSet<>();
		errors.add("is interleaved by table");
		errors.add("is referenced by foreign key");
		StringBuilder sb = new StringBuilder();
		sb.append("TRUNCATE");
		if (Randomly.getBoolean()) {
			sb.append(" TABLE");
		}
		sb.append(" ");
		if (Randomly.getBooleanWithRatherLowProbability()) {
			for (int i = 0; i < Randomly.smallNumber() + 1; i++) {
				if (i != 0) {
					sb.append(", ");
				}
				sb.append(globalState.getSchema().getRandomTable(t -> !t.isView()).getName());
			}
		} else {
			sb.append(globalState.getSchema().getRandomTable(t -> !t.isView()).getName());
		}
		if (Randomly.getBoolean()) {
			sb.append(" ");
			sb.append(Randomly.fromOptions("CASCADE", "RESTRICT"));
		}
		Query q = new QueryAdapter(sb.toString(), errors);
		return q;
	}
	
}
