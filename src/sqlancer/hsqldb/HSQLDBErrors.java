package sqlancer.hsqldb;

import java.util.ArrayList;
import java.util.List;

import sqlancer.common.query.ExpectedErrors;

public final class HSQLDBErrors {

    private HSQLDBErrors() {
    }

    public static List<String> getExpressionErrors() {
        ArrayList<String> errors = new ArrayList<>();

        errors.add("invalid datetime format");
        errors.add("invalid character value for cast");
        errors.add("invalid ORDER BY expression");
        errors.add("data type of expression is not boolean");
        errors.add("numeric value out of range");
        errors.add("incompatible data types in combination");
        errors.add("string data, right truncation");

        return errors;
    }

    public static void addExpressionErrors(ExpectedErrors errors) {
        errors.addAll(getExpressionErrors());
    }

    public static List<String> getInsertErrors() {
        ArrayList<String> errors = new ArrayList<>();

        errors.addAll(getExpressionErrors());

        return errors;
    }

    public static void addInsertErrors(ExpectedErrors errors) {
        errors.addAll(getInsertErrors());
    }

}
