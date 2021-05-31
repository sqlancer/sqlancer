package sqlancer.clickhouse;

import sqlancer.common.query.ExpectedErrors;

public final class ClickHouseErrors {

    private ClickHouseErrors() {
    }

    public static void addExpectedExpressionErrors(ExpectedErrors errors) {
        errors.add("Illegal type");
        errors.add("Argument at index 1 for function like must be constant");
        errors.add("Argument at index 1 for function notLike must be constant");
        errors.add("does not return a value of type UInt8");
        errors.add("invalid escape sequence");
        errors.add("invalid character class range");
        errors.add("Memory limit");
        errors.add("There is no supertype for types");
        errors.add("Bad get: has Int64, requested UInt64");
        errors.add("Cannot convert string");
        errors.add("Cannot read floating point value");
        errors.add("Cannot parse infinity.");
        errors.add("Attempt to read after eof: while converting");
        errors.add(
                "is violated, because it is a constant expression returning 0. It is most likely an error in table definition");
        errors.add("doesn't exist"); // TODO: consecutive test runs can lead to dropped database
        errors.add("is not under aggregate function");
        errors.add("Invalid type for filter in");
        errors.add("argument of function");
        errors.add(" is not under aggregate function and not in GROUP BY");
        errors.add("Expected one of: compound identifier, identifier, list of elements (version"); // VALUES ()
        errors.add("OptimizedRegularExpression: cannot compile re2");
        errors.add("because it is constant but values of constants are different in source and result"); // https://github.com/ClickHouse/ClickHouse/issues/22119
        errors.add("is violated at row 1. Expression:"); // TODO: check constraint on table creation
        errors.add("Cannot parse NaN.: while converting"); // https://github.com/ClickHouse/ClickHouse/issues/22710
        errors.add("Cannot parse number with a sign character but without any numeric character");
        errors.add("Cannot parse number with multiple sign (+/-) characters or intermediate sign character");
    }

    public static void addExpressionHavingErrors(ExpectedErrors errors) {
        errors.add("Memory limit");
    }

    public static void addQueryErrors(ExpectedErrors errors) {
        errors.add("Memory limit");
    }

    public static void addGroupingErrors(ExpectedErrors errors) {
        errors.add("Memory limit");
    }

    public static void addTableManipulationErrors(ExpectedErrors errors) {
        errors.add("Memory limit");
        errors.add("Directory for table data");
        errors.add("Directory not empty");
        errors.add("Partition key cannot contain constants");
        errors.add("Cannot convert string");
        errors.add("argument of function");
        errors.add("Attempt to read after eof: while converting");
        errors.add("Sorting key cannot contain constants");
        errors.add("Sampling expression must be present in the primary key");
        errors.add("Cyclic aliases: default expression and column type are incompatible");
    }

}
