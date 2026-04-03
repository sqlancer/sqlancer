package sqlancer.spark;

import java.util.ArrayList;
import java.util.List;

import sqlancer.common.query.ExpectedErrors;

public final class SparkErrors {

    private SparkErrors() {
    }

    public static List<String> getExpressionErrors() {
        ArrayList<String> errors = new ArrayList<>();

        errors.add("cannot resolve");
        errors.add("AnalysisException");
        errors.add("data type mismatch");
        errors.add("undefined function");
        errors.add("mismatched input");
        errors.add("due to data type mismatch");

        // --- Invalid Literals
        errors.add("The value of the typed literal");

        errors.add("DATATYPE_MISMATCH");
        errors.add("cannot be cast to");

        errors.add("Overflow");
        errors.add("Divide by zero"); // Common if spark.sql.ansi.enabled is true
        errors.add("division by zero");

        // --- Group By / Aggregation errors ---
        errors.add("grouping expressions");
        errors.add("expression is neither present in the group by");
        errors.add("is not a valid grouping expression");
        errors.add("is not contained in either an aggregate function or the GROUP BY clause");

        return errors;
    }

    public static void addExpressionErrors(ExpectedErrors errors) {
        errors.addAll(getExpressionErrors());
    }

    public static List<String> getInsertErrors() {
        ArrayList<String> errors = new ArrayList<>();

        errors.add("not enough data columns");
        errors.add("cannot write to");
        errors.add("incompatible types");
        errors.add("too many data columns");
        errors.add("cannot be cast to");
        errors.add("Error running query");
        errors.add("The value of the typed literal");
        errors.add("Cannot safely cast"); // Found in logs: Decimal -> Date
        errors.add("AnalysisException"); // Spark throws this for almost all insert failures

        return errors;
    }

    public static void addInsertErrors(ExpectedErrors errors) {
        errors.addAll(getInsertErrors());
    }
}