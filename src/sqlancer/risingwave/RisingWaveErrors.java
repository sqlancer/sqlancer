package sqlancer.risingwave;

import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.util.List;

import sqlancer.common.query.ExpectedErrors;

public final class RisingWaveErrors {
    public static final String TIMEOUT_MARKER = "RisingWave query timeout";

    public enum Category {
        NUMERIC_OVERFLOW, TIMEOUT, UNEXPECTED
    }

    private RisingWaveErrors() {
    }

    public static ExpectedErrors expressionErrors() {
        ExpectedErrors errors = new ExpectedErrors();
        addExpressionErrors(errors);
        return errors;
    }

    public static void addExpressionErrors(ExpectedErrors errors) {
        addNumericErrors(errors);
        errors.add(TIMEOUT_MARKER);
    }

    private static void addNumericErrors(ExpectedErrors errors) {
        errors.addAll(List.of("numeric field overflow", "integer out of range", "bigint out of range",
                "smallint out of range", "Numeric out of range"));
    }

    private static ExpectedErrors numericErrors() {
        ExpectedErrors errors = new ExpectedErrors();
        addNumericErrors(errors);
        return errors;
    }

    public static Category classify(SQLException error) {
        boolean rwCancellation = error.getMessage() != null
                && error.getMessage().lines().anyMatch(line -> line.trim().equals("Receive shutdown msg: Cancel")
                        || line.trim().equals("Task aborted: cancelled"));
        if (error instanceof SQLTimeoutException || "57014".equals(error.getSQLState())
                || "XX000".equals(error.getSQLState()) && rwCancellation) {
            return Category.TIMEOUT;
        }
        if (error.getMessage() != null && numericErrors().errorIsExpected(error.getMessage())) {
            return Category.NUMERIC_OVERFLOW;
        }
        return Category.UNEXPECTED;
    }
}
