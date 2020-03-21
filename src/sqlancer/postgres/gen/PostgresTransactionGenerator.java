package sqlancer.postgres.gen;

import java.util.ArrayList;
import java.util.List;

import sqlancer.Query;
import sqlancer.QueryAdapter;
import sqlancer.Randomly;

public class PostgresTransactionGenerator {

	public static Query executeBegin() {
		List<String> errors = new ArrayList<>();
		StringBuilder sb = new StringBuilder("BEGIN");
		if (Randomly.getBoolean()) {
			errors.add("SET TRANSACTION ISOLATION LEVEL must be called before any query");
			sb.append(" ISOLATION LEVEL ");
			sb.append(Randomly.fromOptions("SERIALIZABLE", "REPEATABLE READ", "READ COMMITTED", "READ UNCOMMITTED"));
//			if (Randomly.getBoolean()) {
//				sb.append(" ");
//				sb.append(Randomly.fromOptions("READ WRITE", "READ ONLY"));
//			}
		}
		QueryAdapter query = new QueryAdapter(sb.toString(), errors) {
			@Override
			public boolean couldAffectSchema() {
				return true;
			}
		};	
		return query;
	}

}
