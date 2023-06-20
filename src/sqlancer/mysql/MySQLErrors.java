package sqlancer.mysql;

import sqlancer.common.query.ExpectedErrors;

public final class MySQLErrors {

    private MySQLErrors() {
    }

    public static void addExpressionErrors(ExpectedErrors errors) {
        errors.add("BIGINT value is out of range"); // e.g., CAST(-('-1e500') AS SIGNED)
        errors.add("is not valid for CHARACTER SET");

        if (MySQLBugs.bug111471) {
            errors.add("Memory capacity exceeded");
        }
    }

}
