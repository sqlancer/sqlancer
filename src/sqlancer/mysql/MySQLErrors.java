package sqlancer.mysql;

import java.util.ArrayList;
import java.util.List;

import sqlancer.common.query.ExpectedErrors;

public final class MySQLErrors {

    private MySQLErrors() {
    }

    public static List<String> getExpressionErrors() {
        ArrayList<String> errors = new ArrayList<>();

        errors.add("BIGINT value is out of range"); // e.g., CAST(-('-1e500') AS SIGNED)
        errors.add("is not valid for CHARACTER SET");

        if (MySQLBugs.bug111471) {
            errors.add("Memory capacity exceeded");
        }

        return errors;
    }

    public static void addExpressionErrors(ExpectedErrors errors) {
        errors.addAll(getExpressionErrors());
    }

    public static List<String> getInsertUpdateErrors() {
        ArrayList<String> errors = new ArrayList<>();

        errors.add("doesn't have a default value");
        errors.add("Data truncation");
        errors.add("Incorrect integer value");
        errors.add("Duplicate entry");
        errors.add("Data truncated for column");
        errors.add("Data truncated for functional index");
        errors.add("cannot be null");
        errors.add("Incorrect decimal value");

        return errors;
    }

    public static void addInsertUpdateErrors(ExpectedErrors errors) {
        errors.addAll(getInsertUpdateErrors());
    }

}
