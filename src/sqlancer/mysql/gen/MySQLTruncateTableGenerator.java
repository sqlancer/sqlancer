package sqlancer.mysql.gen;

import java.util.Arrays;

import sqlancer.Query;
import sqlancer.QueryAdapter;
import sqlancer.mysql.MySQLGlobalState;

public class MySQLTruncateTableGenerator {

    private MySQLTruncateTableGenerator() {
    }

    public static Query generate(MySQLGlobalState globalState) {
        StringBuilder sb = new StringBuilder("TRUNCATE TABLE ");
        sb.append(globalState.getSchema().getRandomTable().getName());
        return new QueryAdapter(sb.toString(), Arrays.asList("doesn't have this option"));
    }

}
