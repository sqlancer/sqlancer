package sqlancer.h2;

import sqlancer.common.query.ExpectedErrors;

public final class H2Errors {

    private H2Errors() {
    }

    public static void addInsertErrors(ExpectedErrors errors) {
        errors.add("NULL not allowed for column");
        errors.add("Unique index or primary key violation");
        errors.add("Data conversion error");
    }

    public static void addExpressionErrors(ExpectedErrors errors) {
        errors.add("Numeric value out of range");
        errors.add("are not comparable");
        errors.add("Data conversion error converting");
        errors.add("Feature not supported");
    }

}
