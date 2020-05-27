package sqlancer.mariadb.gen;

import sqlancer.Query;
import sqlancer.QueryAdapter;
import sqlancer.mariadb.MariaDBSchema;

public class MariaDBTruncateGenerator {

    private MariaDBTruncateGenerator() {
    }

    public static Query truncate(MariaDBSchema s) {
        StringBuilder sb = new StringBuilder("TRUNCATE ");
        sb.append(s.getRandomTable().getName());
        sb.append(" ");
        MariaDBCommon.addWaitClause(sb);
        return new QueryAdapter(sb.toString());
    }

}
