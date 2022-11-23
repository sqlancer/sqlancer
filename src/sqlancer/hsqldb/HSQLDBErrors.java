package sqlancer.hsqldb;

import sqlancer.common.query.ExpectedErrors;

public final class HSQLDBErrors {

    private HSQLDBErrors() {
    }

    public static void addExpressionErrors(ExpectedErrors errors) {
    }

    private static void addRegexErrors(ExpectedErrors errors) {
    }

    private static void addFunctionErrors(ExpectedErrors errors) {
    }

    public static void addInsertErrors(ExpectedErrors errors) {
        addRegexErrors(errors);
        addFunctionErrors(errors);
    }

}
