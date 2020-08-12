package sqlancer.mariadb.gen;

import sqlancer.common.query.Query;
import sqlancer.common.query.QueryAdapter;
import sqlancer.mariadb.MariaDBSchema;

public final class MariaDBTruncateGenerator {

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
