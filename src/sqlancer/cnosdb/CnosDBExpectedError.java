package sqlancer.cnosdb;

import java.util.ArrayList;
import java.util.List;

import sqlancer.common.query.ExpectedErrors;

public final class CnosDBExpectedError {
    private static final List<String> ERRORS = new ArrayList<>();

    private CnosDBExpectedError() {
    }

    static {
        ERRORS.add("have the same name. Consider aliasing");
        ERRORS.add(
                "error: Optimizer rule 'projection_push_down' failed due to unexpected error: Schema error: Schema contains duplicate qualified field name");
        ERRORS.add("Projection references non-aggregate values:");
        ERRORS.add("External err: Schema error: No field named");
        ERRORS.add(
                "Optimizer rule 'common_sub_expression_eliminate' failed due to unexpected error: Schema error: No field named");
        ERRORS.add("Binary");
        ERRORS.add("Invalid pattern in LIKE expression");
        ERRORS.add("If the projection contains the time column, it must contain the field column.");
        ERRORS.add("Schema error: No field named");
        ERRORS.add("Optimizer rule 'simplify_expressions' failed due to unexpected error:");
        ERRORS.add("err: Internal error: Optimizer rule 'projection_push_down' failed due to unexpected error");
        ERRORS.add("Schema error: No field named ");
        ERRORS.add("err: External err: Schema error: No field named");
        ERRORS.add("Optimizer rule 'simplify_expressions' failed due to unexpected error");
        ERRORS.add("Csv error: CSV Writer does not support List");
        ERRORS.add("This feature is not implemented: cross join.");
        ERRORS.add("Execution error: field position must be greater than zero");
        ERRORS.add("First argument of `DATE_PART` must be non-null scalar Utf8");
        ERRORS.add("Cannot create filter with non-boolean predicate 'NULL' returning Null");
        ERRORS.add("requested character too large for encoding.");
        ERRORS.add("Can not find compatible types to compare Boolean with [Utf8].");
        ERRORS.add("Cannot create filter with non-boolean predicate 'APPROXDISTINCT");
        ERRORS.add("HAVING clause references non-aggregate values:");
        ERRORS.add("Cannot create filter with non-boolean predicate");
        ERRORS.add("negative substring length not allowed");
        ERRORS.add("The function Sum does not support inputs of type Boolean.");
        ERRORS.add("The function Avg does not support inputs of type Boolean.");
        ERRORS.add("Percentile value must be between 0.0 and 1.0 inclusive");
        ERRORS.add("Date part '' not supported");
        ERRORS.add("Min/Max accumulator not implemented for type Boolean.");
        ERRORS.add("meta need get_series_id_by_filter");
        ERRORS.add("Arrow: Cast error:");
        ERRORS.add("Arrow error: Cast error:");
        ERRORS.add("Datafusion: Execution error: Arrow error: External error: Arrow error: Cast error:");
        ERRORS.add("Arrow error: Divide by zero error");
        ERRORS.add("desired percentile argument must be float literal");
        ERRORS.add("Unsupported CAST from Int32 to Timestamp(Nanosecond, None)");
        ERRORS.add("Execution error: Date part");
        ERRORS.add("Physical plan does not support logical expression MIN(Boolean");
        ERRORS.add("The percentile argument for ApproxPercentileCont must be Float64, not Int64");
        ERRORS.add("The percentile argument for ApproxPercentileContWithWeight must be Float64, not Int64.");
        ERRORS.add("Data type UInt64 not supported for binary operation '#' on dyn arrays.");
        ERRORS.add("Arrow: Divide by zero error");
        ERRORS.add("The function ApproxPercentileCont does not support inputs of type Null.");
        ERRORS.add("can't be evaluated because there isn't a common type to coerce the types to");
        ERRORS.add("This was likely caused by a bug in DataFusion's code and we would welcome that you file an bug");
        ERRORS.add("The function ApproxMedian does not support inputs of type Null.");
        ERRORS.add("null character not permitted.");
        ERRORS.add("The percentile argument for ApproxPercentileCont must be Float64, not Null.");
        ERRORS.add("This feature is not implemented");
        ERRORS.add("The function Avg does not support inputs of type Null.");
        ERRORS.add("Coercion from [Utf8, Timestamp(Nanosecond, Some(\\\"+00:00\\\"))]");
        ERRORS.add(
                "Coercion from [Utf8, Float64, Utf8] to the signature OneOf([Exact([Utf8, Int64]), Exact([LargeUtf8, Int64]), Exact([Utf8, Int64, Utf8]), Exact([LargeUtf8, Int64, Utf8]), Exact([Utf8, Int64, LargeUtf8]), Exact([LargeUtf8, Int64, LargeUtf8])]) failed.");
        ERRORS.add("Coercion from");
        ERRORS.add("Build left right indices error");
        ERRORS.add("Schema contains duplicate unqualified field name 'time'");
    }

    public static ExpectedErrors expectedErrors() {
        ExpectedErrors res = new ExpectedErrors();
        res.addAll(ERRORS);
        return res;
    }

}
