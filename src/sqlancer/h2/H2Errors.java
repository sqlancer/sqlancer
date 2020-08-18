package sqlancer.h2;

import sqlancer.common.query.ExpectedErrors;

public final class H2Errors {

    private H2Errors() {
    }

    public static void addInsertErrors(ExpectedErrors errors) {
        errors.add("NULL not allowed for column");
        errors.add("Unique index or primary key violation");
        errors.add("Data conversion error");
        errors.add("Generated column");
        errors.add("Value too long for column");
    }

    public static void addExpressionErrors(ExpectedErrors errors) {
        errors.add("Numeric value out of range");
        errors.add("are not comparable");
        errors.add("Data conversion error converting");
        errors.add("Feature not supported");

        errors.add("must be in the GROUP BY list");
        errors.add("must be in the result list in this case"); // ORDER BY
        errors.add("Division by zero");
        errors.add("for parameter \"numeric\";"); // Invalid value "CHARACTER VARYING(1)" for parameter "numeric"; SQL
                                                  // statement:

        // regexp
        errors.add("Unclosed group near index");
        errors.add("Error in LIKE ESCAPE");
    }

}
