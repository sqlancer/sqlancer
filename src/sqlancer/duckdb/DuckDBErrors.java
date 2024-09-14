package sqlancer.duckdb;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import sqlancer.common.query.ExpectedErrors;

public final class DuckDBErrors {

    private DuckDBErrors() {
    }

    public static List<String> getExpressionErrors() {
        ArrayList<String> errors = new ArrayList<>();

        errors.add("with non-constant precision is not supported");
        errors.add("Like pattern must not end with escape character");
        errors.add("Could not convert string");
        errors.add("ORDER term out of range - should be between ");
        errors.add("You might need to add explicit type casts.");
        errors.add("can't be cast because the value is out of range for the destination type");
        errors.add("field value out of range");
        errors.add("Unimplemented type for cast");

        errors.add("Escape string must be empty or one character.");
        errors.add("Type mismatch when combining rows"); // BETWEEN

        errors.add("invalid UTF-8"); // TODO
        errors.add("String value is not valid UTF8");

        errors.add("Invalid TypeId "); // TODO

        errors.add("GROUP BY clause cannot contain aggregates!"); // investigate

        errors.addAll(getRegexErrors());

        errors.addAll(getFunctionErrors());

        errors.add("Overflow in multiplication");
        errors.add("Out of Range");
        errors.add("Date out of range");

        // collate
        errors.add("Cannot combine types with different collation!");
        errors.add("collations are only supported for type varchar");

        errors.add("Like pattern must not end with escape character!"); // LIKE

        errors.add("does not have a column named"); // TODO: this only happens for views whose underlying table has a
                                                    // removed column
        errors.add("Contents of view were altered: types don't match!");
        errors.add("Not implemented: ROUND(DECIMAL, INTEGER) with non-constant precision is not supported");
        errors.add("ORDER BY non-integer literal has no effect");

        // timestamp
        errors.add("Cannot subtract infinite timestamps");
        errors.add("Timestamp difference is out of bounds");

        return errors;
    }

    public static List<Pattern> getExpressionErrorsRegex() {
        ArrayList<Pattern> errors = new ArrayList<>();

        errors.add(Pattern.compile("Binder Error: Cannot mix values of type .* and .* in BETWEEN clause"));
        errors.add(Pattern.compile("Binder Error: Cannot mix values of type .* and .* in CASE expression"));
        errors.add(Pattern.compile("Cannot mix values of type .* and .* in COALESCE operator"));
        errors.add(Pattern.compile("Cannot compare values of type .* and type .*"));

        return errors;
    }

    public static void addExpressionErrors(ExpectedErrors errors) {
        errors.addAll(getExpressionErrors());
        errors.addAllRegexes(getExpressionErrorsRegex());
    }

    private static List<String> getRegexErrors() {
        ArrayList<String> errors = new ArrayList<>();

        errors.add("Invalid Input Error:");
        return errors;
    }

    private static List<String> getFunctionErrors() {
        ArrayList<String> errors = new ArrayList<>();

        errors.add("SUBSTRING cannot handle negative lengths");
        errors.add("is undefined outside [-1,1]"); // ACOS etc
        errors.add("invalid type specifier"); // PRINTF
        errors.add("argument index out of range"); // PRINTF
        errors.add("invalid format string"); // PRINTF
        errors.add("number is too big"); // PRINTF
        errors.add("Like pattern must not end with escape character!"); // LIKE
        errors.add("Could not choose a best candidate function for the function call \"date_part"); // date_part
        errors.add("extract specifier"); // date_part
        errors.add("not recognized"); // date_part
        errors.add("not supported"); // date_part
        errors.add("Failed to cast");
        errors.add("Conversion Error");
        errors.add("Could not cast value");
        errors.add("Insufficient padding in RPAD"); // RPAD
        errors.add("Could not choose a best candidate function for the function call"); // monthname
        errors.add("expected a numeric precision field"); // ROUND
        errors.add("with non-constant precision is not supported"); // ROUND

        return errors;
    }

    public static List<String> getInsertErrors() {
        ArrayList<String> errors = new ArrayList<>();

        errors.addAll(getRegexErrors());
        errors.addAll(getFunctionErrors());

        errors.add("NOT NULL constraint failed");
        errors.add("PRIMARY KEY or UNIQUE constraint violated");
        errors.add("Duplicate key");
        errors.add("can't be cast because the value is out of range for the destination type");
        errors.add("Could not convert string");
        errors.add("Unimplemented type for cast");
        errors.add("field value out of range");
        errors.add("CHECK constraint failed");
        errors.add("Cannot explicitly insert values into rowid column"); // TODO: don't insert into rowid
        errors.add(" Column with name rowid does not exist!"); // currently, there doesn't seem to way to determine if
                                                               // the table has a primary key
        errors.add("Could not cast value");
        errors.add("create unique index, table contains duplicate data");
        errors.add("Failed to cast");

        return errors;
    }

    public static void addInsertErrors(ExpectedErrors errors) {
        errors.addAll(getInsertErrors());
    }

    public static List<String> getGroupByErrors() {
        ArrayList<String> errors = new ArrayList<>();

        errors.add("must appear in the GROUP BY clause or must be part of an aggregate function");
        errors.add("must appear in the GROUP BY clause or be used in an aggregate function");
        errors.add("GROUP BY term out of range");

        return errors;
    }

    public static void addGroupByErrors(ExpectedErrors errors) {
        errors.addAll(getGroupByErrors());
    }

}
