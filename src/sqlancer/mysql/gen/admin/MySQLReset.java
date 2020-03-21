package sqlancer.mysql.gen.admin;

import java.util.stream.Collectors;

import sqlancer.Query;
import sqlancer.QueryAdapter;
import sqlancer.Randomly;

public class MySQLReset {

	public static Query create() {
		StringBuilder sb = new StringBuilder();
		sb.append("RESET ");
		sb.append(Randomly.nonEmptySubset("MASTER", "SLAVE").stream().collect(Collectors.joining(", ")));
		return new QueryAdapter(sb.toString());
	}

}
