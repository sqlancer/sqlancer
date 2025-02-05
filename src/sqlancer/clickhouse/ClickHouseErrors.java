package sqlancer.clickhouse;

import java.util.List;

import sqlancer.common.query.ExpectedErrors;

public final class ClickHouseErrors {

    private ClickHouseErrors() {
    }

    public static List<String> getExpectedExpressionErrors() {
        return List.of("Argument at index 1 for function like must be constant",
                "Argument at index 1 for function notLike must be constant",
                "Attempt to read after eof: while converting", "Bad get: has Int64, requested UInt64",
                "Cannot convert string", "Cannot insert NULL value into a column of type",
                "Cannot parse Int32 from String, because value is too short", "Cannot parse NaN.: while converting", // https://github.com/ClickHouse/ClickHouse/issues/22710
                "Cannot parse infinity.", "Cannot parse number with a sign character but without any numeric character",
                "Cannot parse number with multiple sign (+/-) characters or intermediate sign character",
                "Cannot parse string", "Cannot read floating point value",
                "Cyclic aliases: default expression and column type are incompatible", "Directory for table data",
                "Directory not empty", "Expected one of: compound identifier, identifier, list of elements (version", // VALUES
                                                                                                                      // ()
                "Function 'like' doesn't support search with non-constant needles in constant haystack", "Illegal type",
                "Illegal value (aggregate function) for positional argument in GROUP BY",
                "Invalid escape sequence at the end of LIKE pattern", "Invalid type for filter in", "Memory limit",
                "OptimizedRegularExpression: cannot compile re2", "Partition key cannot contain constants",
                "Positional argument out of bounds", "Sampling expression must be present in the primary key",
                "Sorting key cannot contain constants", "There is no supertype for types", "argument of function",
                "but its arguments considered equal according to constraints", "does not return a value of type UInt8",
                "doesn't exist", // TODO: consecutive test runs can lead to dropped database
                "in block. There are only columns:", // https://github.com/ClickHouse/ClickHouse/issues/42399
                "invalid character class range", "invalid escape sequence",
                "is not under aggregate function and not in GROUP BY", "is not under aggregate function",
                "is violated at row 1. Expression:", // TODO: check constraint on table creation
                "is violated, because it is a constant expression returning 0. It is most likely an error in table definition",
                "there are only columns", "there are columns", "(NOT_FOUND_COLUMN_IN_BLOCK)", "Missing columns",
                "Ambiguous column", "Must be one unsigned integer type. (ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER)",
                "Floating point partition key is not supported", "Cannot get JOIN keys from JOIN ON section",
                "ILLEGAL_DIVISION", "DECIMAL_OVERFLOW",
                "Cannot convert out of range floating point value to integer type",
                "Unexpected inf or nan to integer conversion", "No such name in Block::erase", // https://github.com/ClickHouse/ClickHouse/issues/42769
                "EMPTY_LIST_OF_COLUMNS_QUERIED", // https://github.com/ClickHouse/ClickHouse/issues/43003
                "cannot get JOIN keys. (INVALID_JOIN_ON_EXPRESSION)", "AMBIGUOUS_IDENTIFIER", "CYCLIC_ALIASES",
                "Positional argument numeric constant expression is not representable as",
                "Positional argument must be constant with numeric type", " is out of bounds. Expected in range",
                "with constants is not supported. (INVALID_JOIN_ON_EXPRESSION)",
                "Cannot get JOIN keys from JOIN ON section", "Unexpected inf or nan to integer conversion",
                "Cannot determine join keys in", "Unsigned type must not contain",
                "Unexpected inf or nan to integer conversion",

                // The way we generate JOINs we can have ambiguous left table column without
                // alias
                // We may not count it as an issue, but it makes no sense to add more complex
                // AST generation logic
                "MULTIPLE_EXPRESSIONS_FOR_ALIAS", "AMBIGUOUS_IDENTIFIER", // https://github.com/ClickHouse/ClickHouse/issues/45389
                "AMBIGUOUS_COLUMN_NAME", // same https://github.com/ClickHouse/ClickHouse/issues/45389
                "No equality condition found in JOIN ON expression", "Cannot parse number with multiple sign");
    }

    public static void addExpectedExpressionErrors(ExpectedErrors errors) {
        errors.addAll(getExpectedExpressionErrors());
    }

}
