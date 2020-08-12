package sqlancer.mysql.gen;

import sqlancer.ExpectedErrors;
import sqlancer.Query;
import sqlancer.QueryAdapter;
import sqlancer.mysql.MySQLGlobalState;

public final class MySQLTruncateTableGenerator {

    private MySQLTruncateTableGenerator() {
    }

    public static Query generate(MySQLGlobalState globalState) {
        StringBuilder sb = new StringBuilder("TRUNCATE TABLE ");
        sb.append(globalState.getSchema().getRandomTable().getName());
        return new QueryAdapter(sb.toString(), ExpectedErrors.from("doesn't have this option"));
    }

}
