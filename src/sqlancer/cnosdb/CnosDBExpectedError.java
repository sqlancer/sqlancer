package sqlancer.cnosdb;

import java.util.ArrayList;
import java.util.List;

public final class CnosDBExpectedError {
    private CnosDBExpectedError() {
    }

    public static List<String> expectedErrors() {
        List<String> errors = new ArrayList<>();
        errors.add("have the same name. Consider aliasing");
        errors.add(
                "error: Optimizer rule 'projection_push_down' failed due to unexpected error: Schema error: Schema contains duplicate qualified field name");
        errors.add("Projection references non-aggregate values:");
        errors.add("External err: Schema error: No field named");
        errors.add(
                "Optimizer rule 'common_sub_expression_eliminate' failed due to unexpected error: Schema error: No field named");
        errors.add("Binary");
        errors.add("Invalid pattern in LIKE expression");
        errors.add("If the projection contains the time column, it must contain the field column.");
        errors.add("Schema error: No field named");
        errors.add("Optimizer rule 'simplify_expressions' failed due to unexpected error:");
        errors.add("err: Internal error: Optimizer rule 'projection_push_down' failed due to unexpected error");
        errors.add("Schema error: No field named ");
        errors.add("err: External err: Schema error: No field named");
        errors.add("Optimizer rule 'simplify_expressions' failed due to unexpected error");
        errors.add("Csv error: CSV Writer does not support List");
        errors.add("This feature is not implemented: cross join.");
        errors.add("Execution error: field position must be greater than zero");
        errors.add("First argument of `DATE_PART` must be non-null scalar Utf8");
        errors.add("Cannot create filter with non-boolean predicate 'NULL' returning Null");
        errors.add("requested character too large for encoding.");
        errors.add("Can not find compatible types to compare Boolean with [Utf8].");
        errors.add("Cannot create filter with non-boolean predicate 'APPROXDISTINCT");
        errors.add("HAVING clause references non-aggregate values:");
        errors.add("Cannot create filter with non-boolean predicate");
        errors.add("negative substring length not allowed");
        errors.add("The function Sum does not support inputs of type Boolean.");
        errors.add("The function Avg does not support inputs of type Boolean.");
        errors.add("Percentile value must be between 0.0 and 1.0 inclusive");
        errors.add("Date part '' not supported");
        errors.add("Min/Max accumulator not implemented for type Boolean.");
        errors.add("meta need get_series_id_by_filter");
        errors.add("Arrow: Cast error:");
        errors.add("Arrow error: Cast error:");
        errors.add("Datafusion: Execution error: Arrow error: External error: Arrow error: Cast error:");
        errors.add("Arrow error: Divide by zero error");
        errors.add("desired percentile argument must be float literal");
        errors.add("Unsupported CAST from Int32 to Timestamp(Nanosecond, None)");
        errors.add("Execution error: Date part");
        errors.add("Physical plan does not support logical expression MIN(Boolean");
        errors.add("The percentile argument for ApproxPercentileCont must be Float64, not Int64");
        errors.add("The percentile argument for ApproxPercentileContWithWeight must be Float64, not Int64.");
        errors.add("Data type UInt64 not supported for binary operation '#' on dyn arrays.");
        errors.add("Arrow: Divide by zero error");
        errors.add("The function ApproxPercentileCont does not support inputs of type Null.");
        errors.add("can't be evaluated because there isn't a common type to coerce the types to");
        errors.add("This was likely caused by a bug in DataFusion's code and we would welcome that you file an bug");
        errors.add("The function ApproxMedian does not support inputs of type Null.");
        errors.add("null character not permitted.");
        errors.add("The percentile argument for ApproxPercentileCont must be Float64, not Null.");
        errors.add("This feature is not implemented");
        errors.add("The function Avg does not support inputs of type Null.");
        errors.add("Coercion from [Utf8, Timestamp(Nanosecond, Some(\\\"+00:00\\\"))]");
        errors.add(
                "Coercion from [Utf8, Float64, Utf8] to the signature OneOf([Exact([Utf8, Int64]), Exact([LargeUtf8, Int64]), Exact([Utf8, Int64, Utf8]), Exact([LargeUtf8, Int64, Utf8]), Exact([Utf8, Int64, LargeUtf8]), Exact([LargeUtf8, Int64, LargeUtf8])]) failed.");
        errors.add("Coercion from");
        return errors;
    }

}
