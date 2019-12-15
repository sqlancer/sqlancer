package lama.mariadb.gen;

import lama.Query;
import lama.QueryAdapter;
import lama.mariadb.MariaDBSchema;

public class MariaDBTruncateGenerator {
	
	public static Query truncate(MariaDBSchema s) {
		StringBuilder sb = new StringBuilder("TRUNCATE ");
		sb.append(s.getRandomTable().getName());
		sb.append(" ");
		MariaDBCommon.addWaitClause(sb);
		return new QueryAdapter(sb.toString());
	}

}
