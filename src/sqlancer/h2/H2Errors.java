package sqlancer.h2;

import sqlancer.common.query.ExpectedErrors;

public final class H2Errors {

    private H2Errors() {
    }

    public static void addInsertErrors(ExpectedErrors errors) {
    }

    public static void addExpressionErrors(ExpectedErrors errors) {
        errors.add("Numeric value out of range");
        errors.add("are not comparable");
    }

}
