package sqlancer.mysql;

import java.util.Set;

public class MySQLErrors {

    private MySQLErrors() {
    }

    public static void addExpressionErrors(Set<String> errors) {
        errors.add("BIGINT value is out of range"); // e.g., CAST(-('-1e500') AS SIGNED)
        errors.add("is not valid for CHARACTER SET");
    }

}
