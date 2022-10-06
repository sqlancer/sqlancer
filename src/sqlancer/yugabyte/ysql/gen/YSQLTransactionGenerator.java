package sqlancer.yugabyte.ysql.gen;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;

public final class YSQLTransactionGenerator {

    private YSQLTransactionGenerator() {
    }

    public static SQLQueryAdapter executeBegin() {
        ExpectedErrors errors = new ExpectedErrors();
        StringBuilder sb = new StringBuilder("BEGIN");
        if (Randomly.getBoolean()) {
            errors.add("SET TRANSACTION ISOLATION LEVEL must be called before any query");
            sb.append(" ISOLATION LEVEL ");
            sb.append(Randomly.fromOptions("SERIALIZABLE", "REPEATABLE READ", "READ COMMITTED"));
            // if (Randomly.getBoolean()) {
            // sb.append(" ");
            // sb.append(Randomly.fromOptions("READ WRITE", "READ ONLY"));
            // }
        }
        return new SQLQueryAdapter(sb.toString(), errors, true);
    }

}
