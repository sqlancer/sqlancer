package sqlancer.clickhouse;

import java.util.Set;

public final class ClickHouseErrors {

    private ClickHouseErrors() {
    }

    public static void addExpectedExpressionErrors(Set<String> errors) {
        // errors.add("Illegal type (String) of argument of function not");
        // errors.add("Illegal type String of column for constant filter. Must be UInt8 or Nullable(UInt8)");
        // errors.add("Illegal type Int32 of column for constant filter. Must be UInt8 or Nullable(UInt8)");
        // errors.add("Illegal type UInt32 of column for constant filter. Must be UInt8 or Nullable(UInt8)");
        // errors.add("Illegal type Int32 of column for filter. Must be UInt8 or Nullable(UInt8) or Const variants of
        // them.");
        // errors.add("Illegal type String of column for filter. Must be UInt8 or Nullable(UInt8) or Const variants of
        // them.");
        // errors.add("Illegal type Int64 of column for constant filter. Must be UInt8 or Nullable(UInt8)");
        errors.add("Illegal type");
        errors.add("Argument at index 1 for function like must be constant");
        errors.add("Argument at index 1 for function notLike must be constant");

        // regex
        errors.add("invalid escape sequence");
        errors.add("invalid character class range");
        errors.add("Memory limit");
        errors.add("There is no supertype for types");
        errors.add("Bad get: has Int64, requested UInt64");
        errors.add("Cannot convert string");
        errors.add("Cannot read floating point value");
        errors.add("Cannot parse infinity.");
        errors.add("Attempt to read after eof: while converting");
        errors.add("doesn't exist"); // TODO: consecutive test runs can lead to dropped database
        errors.add("is not under aggregate function");
        errors.add("Invalid type for filter in");
        errors.add("argument of function");
    }

    public static void addExpressionHavingErrors(Set<String> errors) {
        errors.add("Memory limit");
    }

    public static void addQueryErrors(Set<String> errors) {
        errors.add("Memory limit");
    }

    public static void addGroupingErrors(Set<String> errors) {
        errors.add("Memory limit");
    }

    public static void addTableManipulationErrors(Set<String> errors) {
        errors.add("Memory limit");
        errors.add("Directory for table data");
        errors.add("Directory not empty");
        errors.add("Partition key cannot contain constants");
        errors.add("Cannot convert string");
        errors.add("argument of function");
        errors.add("Attempt to read after eof: while converting");
        errors.add("Sorting key cannot contain constants");
        errors.add("Sampling expression must be present in the primary key");
    }

}
