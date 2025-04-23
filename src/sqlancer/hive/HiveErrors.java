package sqlancer.hive;

import java.util.ArrayList;
import java.util.List;

import sqlancer.common.query.ExpectedErrors;

public class HiveErrors {

    private HiveErrors() {
    }

    public static List<String> getExpressionErrors() {
        ArrayList<String> errors = new ArrayList<>();

        errors.add("cannot recognize input near");
        errors.add("Argument type mismatch");
        errors.add("Error while compiling statement");

        return errors;
    }

    public static void addExpressionErrors(ExpectedErrors errors) {
        errors.addAll(getExpressionErrors());
    }

    public static List<String> getInsertErrors() {
        ArrayList<String> errors = new ArrayList<>();

        errors.add("Either CHECK or NOT NULL constraint violated!");
        errors.add("Error running query");
        errors.add("is different from preceding arguments");

        return errors;
    }

    public static void addInsertErrors(ExpectedErrors errors) {
        errors.addAll(getInsertErrors());
    }
}
