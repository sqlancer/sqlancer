package sqlancer.mysql.gen.admin;

import java.util.stream.Collectors;

import sqlancer.Query;
import sqlancer.QueryAdapter;
import sqlancer.Randomly;
import sqlancer.mysql.MySQLGlobalState;

public class MySQLReset {

	public static Query create(MySQLGlobalState globalState) {
		StringBuilder sb = new StringBuilder();
		sb.append("RESET ");
		sb.append(Randomly.nonEmptySubset("MASTER", "SLAVE").stream().collect(Collectors.joining(", ")));
		return new QueryAdapter(sb.toString());
	}

}
