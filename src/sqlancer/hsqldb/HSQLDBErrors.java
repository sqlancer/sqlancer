package sqlancer.hsqldb;

import java.util.ArrayList;
import java.util.List;

import sqlancer.common.query.ExpectedErrors;

public final class HSQLDBErrors {

    private HSQLDBErrors() {
    }

    public static List<String> getExpressionErrors() {
        ArrayList<String> errors = new ArrayList<>();
        errors.add("");
        return errors;
    }

    public static void addExpressionErrors(ExpectedErrors errors) {
        errors.addAll(getExpressionErrors());
    }

    private static List<String> getRegexErrors() {
        ArrayList<String> errors = new ArrayList<>();

        errors.add("");

        return errors;
    }

    private static List<String> getFunctionErrors() {
        ArrayList<String> errors = new ArrayList<>();

        errors.add("");

        return errors;
    }

    public static List<String> getInsertErrors() {
        ArrayList<String> errors = new ArrayList<>();

        errors.addAll(getRegexErrors());
        errors.addAll(getFunctionErrors());

        return errors;
    }

    public static void addInsertErrors(ExpectedErrors errors) {
        errors.addAll(getInsertErrors());
    }

}
