package lama.mysql.gen.admin;

import java.util.stream.Collectors;

import lama.Query;
import lama.QueryAdapter;
import lama.Randomly;

public class MySQLReset {

	public static Query create() {
		StringBuilder sb = new StringBuilder();
		sb.append("RESET ");
		sb.append(Randomly.nonEmptySubset("MASTER", "SLAVE").stream().collect(Collectors.joining(", ")));
		return new QueryAdapter(sb.toString());
	}

}
