package sqlancer.mariadb.gen;

import sqlancer.common.query.ExpectedErrors;
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
        ExpectedErrors errors = new ExpectedErrors();
        errors.add("Lock wait timeout exceeded; try restarting transaction");
        return new SQLQueryAdapter(sb.toString(), errors);
    }

}
