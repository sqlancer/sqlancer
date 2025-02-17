package sqlancer.yugabyte.ysql;

import java.util.ArrayList;
import java.util.List;

import sqlancer.common.DBMSCommon;
import sqlancer.common.query.ExpectedErrors;

public final class YSQLErrors {

    private YSQLErrors() {
    }

    public static List<String> getCommonFetchErrors() {
        List<String> errors = DBMSCommon.getCommonFetchErrors();

        errors.add("An I/O error occurred while sending to the backend");
        errors.add("Conflicts with committed transaction");
        errors.add("cannot be changed");
        errors.add("SET TRANSACTION ISOLATION LEVEL must be called before any query");

        errors.add("non-integer constant in");
        errors.add("must appear in the GROUP BY clause or be used in an aggregate function");
        errors.add("GROUP BY position");

        return errors;
    }

    public static void addCommonFetchErrors(ExpectedErrors errors) {
        errors.addAll(getCommonFetchErrors());
    }

    public static List<String> getCommonTableErrors() {
        ArrayList<String> errors = new ArrayList<>();

        errors.add("PRIMARY KEY containing column of type 'INET' not yet supported");
        errors.add("PRIMARY KEY containing column of type 'VARBIT' not yet supported");
        errors.add("PRIMARY KEY containing column of type 'INT4RANGE' not yet supported");
        errors.add("INDEX on column of type 'INET' not yet supported");
        errors.add("INDEX on column of type 'VARBIT' not yet supported");
        errors.add("INDEX on column of type 'INT4RANGE' not yet supported");
        errors.add("is not commutative"); // exclude
        errors.add("cannot be changed");
        errors.add("operator requires run-time type coercion"); // exclude

        return errors;
    }

    public static void addCommonTableErrors(ExpectedErrors errors) {
        errors.addAll(getCommonTableErrors());
    }

    public static List<String> getCommonExpressionErrors() {
        List<String> errors = DBMSCommon.getCommonExpressionErrors();

        errors.add("syntax error at or near \"(\"");
        errors.add("does not exist");
        errors.add("is not unique");
        errors.add("cannot be changed");
        errors.add("invalid reference to FROM-clause entry for table");
        errors.add("Invalid column number");
        errors.add("specified more than once");
        errors.add("invalid input syntax for integer");
        errors.add("cannot convert infinity to numeric");

        errors.addAll(getFunctionErrors());

        return errors;
    }

    public static void addCommonExpressionErrors(ExpectedErrors errors) {
        errors.addAll(getCommonExpressionErrors());
    }

    public static void addToCharFunctionErrors(ExpectedErrors errors) {
        errors.addAll(DBMSCommon.getToCharFunctionErrors());
    }

    public static void addBitStringOperationErrors(ExpectedErrors errors) {
        errors.addAll(DBMSCommon.getBitStringOperationErrors());
    }

    public static List<String> getFunctionErrors() {
        List<String> errors = DBMSCommon.getFunctionErrors();
        errors.add("encoding conversion from UTF8 to ASCII not supported"); // to_ascii
        return errors;
    }

    public static void addFunctionErrors(ExpectedErrors errors) {
        errors.addAll(getFunctionErrors());
    }

    public static void addCommonRegexExpressionErrors(ExpectedErrors errors) {
        errors.addAll(DBMSCommon.getCommonRangeExpressionErrors());
    }

    public static void addCommonRangeExpressionErrors(ExpectedErrors errors) {
        errors.addAll(DBMSCommon.getCommonRangeExpressionErrors());
    }

    public static void addCommonInsertUpdateErrors(ExpectedErrors errors) {
        errors.add("value too long for type character");
        errors.add("not found in view targetlist");
    }

    public static List<String> getGroupingErrors() {
        ArrayList<String> errors = new ArrayList<>();

        errors.add("non-integer constant in GROUP BY"); // TODO
        errors.add("must appear in the GROUP BY clause or be used in an aggregate function");
        errors.add("is not in select list");
        errors.add("aggregate functions are not allowed in GROUP BY");

        return errors;
    }

    public static void addGroupingErrors(ExpectedErrors errors) {
        errors.addAll(getGroupingErrors());
    }

    public static List<String> getViewErrors() {
        ArrayList<String> errors = new ArrayList<>();

        errors.add("already exists");
        errors.add("cannot drop columns from view");
        errors.add("non-integer constant in ORDER BY"); // TODO
        errors.add("for SELECT DISTINCT, ORDER BY expressions must appear in select list"); // TODO
        errors.add("cannot change data type of view column");
        errors.add("specified more than once"); // TODO
        errors.add("materialized views must not use temporary tables or views");
        errors.add("does not have the form non-recursive-term UNION [ALL] recursive-term");
        errors.add("is not a view");
        errors.add("non-integer constant in DISTINCT ON");
        errors.add("SELECT DISTINCT ON expressions must match initial ORDER BY expressions");

        return errors;
    }

    public static void addViewErrors(ExpectedErrors errors) {
        errors.addAll(getViewErrors());
    }
}
