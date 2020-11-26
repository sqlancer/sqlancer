package sqlancer.mariadb.gen;

import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.mariadb.MariaDBSchema;

public final class MariaDBTruncateGenerator {

    private MariaDBTruncateGenerator() {
    }

    public static SQLQueryAdapter truncate(MariaDBSchema s) {
        StringBuilder sb = new StringBuilder("TRUNCATE ");
        sb.append(s.getRandomTable().getName());
        sb.append(" ");
        MariaDBCommon.addWaitClause(sb);
        return new SQLQueryAdapter(sb.toString());
    }

}
