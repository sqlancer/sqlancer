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
        errors.add("Referential integrity constraint violation");
        errors.add("Check constraint invalid");
        errors.add("Check constraint violation");
    }

    public static void addExpressionErrors(ExpectedErrors errors) {
        errors.add("java.lang.ArithmeticException: BigInteger would overflow supported range");
        errors.add("Value too long for column");
        errors.add("Numeric value out of range");
        errors.add("are not comparable");
        errors.add("Data conversion error converting");
        errors.add("Feature not supported");

        errors.add("must be in the GROUP BY list");
        errors.add("must be in the result list in this case"); // ORDER BY
        errors.add("Division by zero");

        // regexp
        errors.add("Unclosed group near index");
        errors.add("Error in LIKE ESCAPE");

        // functions
        errors.add("Invalid value" /* ... for parameter */);

        errors.add("String format error"); // STRINGDECODE
        errors.add(/* precision */ "must be between"); // TRUNCATE_VALUE
        errors.add("Cannot parse \"TIMESTAMP\" constant"); // TRUNCATE
        errors.add("Invalid parameter count for \"TRUNC\", expected count: \"1\""); // TRUNCATE
    }

    public static void addDeleteErrors(ExpectedErrors errors) {
        errors.add("No default value is set for column"); // referential actions
        errors.add("Referential integrity constraint violation");
        errors.add("NULL not allowed for column");
    }

}
