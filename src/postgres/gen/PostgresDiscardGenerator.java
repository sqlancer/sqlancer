package postgres.gen;

import lama.Query;
import lama.QueryAdapter;
import lama.Randomly;

public class PostgresDiscardGenerator {

	public static Query create() {
		StringBuilder sb = new StringBuilder();
		sb.append("DISCARD ");
		sb.append(Randomly.fromOptions("ALL", "PLANS", "SEQUENCES", "TEMPORARY", "TEMP"));
		return new QueryAdapter(sb.toString());
	}

}
