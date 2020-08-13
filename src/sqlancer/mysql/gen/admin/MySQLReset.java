package sqlancer.mysql.gen.admin;

import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.query.Query;
import sqlancer.common.query.QueryAdapter;
import sqlancer.mysql.MySQLGlobalState;

public final class MySQLReset {

    private MySQLReset() {
    }

    public static Query create(MySQLGlobalState globalState) {
        StringBuilder sb = new StringBuilder();
        sb.append("RESET ");
        sb.append(Randomly.nonEmptySubset("MASTER", "SLAVE").stream().collect(Collectors.joining(", ")));
        return new QueryAdapter(sb.toString());
    }

}
