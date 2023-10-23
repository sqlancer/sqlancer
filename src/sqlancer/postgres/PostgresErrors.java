package sqlancer.postgres;

import java.util.regex.Pattern;

import sqlancer.common.query.ExpectedErrors;

public final class PostgresErrors {

    private PostgresErrors() {
    }

    public static void addExpressionErrors(ExpectedErrors errors) {
        errors.addRegex(Pattern.compile("ERROR: *"));
    }

}
