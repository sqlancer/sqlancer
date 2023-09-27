package sqlancer.hsqldb;

import sqlancer.common.query.ExpectedErrors;

public final class HSQLDBErrors {

    private HSQLDBErrors() {
    }

    public static void addExpressionErrors(ExpectedErrors errors) {
        errors.add("");
    }

    private static void addRegexErrors(ExpectedErrors errors) {
        errors.add("");
    }

    private static void addFunctionErrors(ExpectedErrors errors) {
        errors.add("");
    }

    public static void addInsertErrors(ExpectedErrors errors) {
        addRegexErrors(errors);
        addFunctionErrors(errors);
    }

}
