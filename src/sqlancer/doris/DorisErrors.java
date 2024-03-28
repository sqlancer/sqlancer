package sqlancer.doris;

import java.util.ArrayList;
import java.util.List;

import sqlancer.common.query.ExpectedErrors;

public final class DorisErrors {

    private DorisErrors() {
    }

    public static List<String> getExpressionErrors() {
        ArrayList<String> errors = new ArrayList<>();

        // SQL syntax error
        errors.add("Syntax error");
        errors.add("Please check your sql, we meet an error when parsing");
        errors.add("but returns type");
        errors.add("is not a number");
        errors.add("not produced by aggregation output");
        errors.add("Can not set null default value to non nullable column");
        errors.add("ordinal must be");
        errors.add("ordinal exceeds number");
        errors.add("items in select list");
        errors.add("java.lang.IllegalStateException: null");
        errors.add("list expression not");
        errors.add("missing from");

        // Not in line with Doris' logic
        errors.add("Unexpected exception: null");
        errors.add("Cross join can't be used with ON clause");
        errors.add("BetweenPredicate needs to be rewritten into a CompoundPredicate");
        errors.add("can't be assigned to some PlanNode");
        errors.add("can not cast from origin type");
        errors.add("not produced by aggregation output");
        errors.add("cannot combine"); // cannot combine SELECT DISTINCT with aggregate functions or GROUP BY
        errors.add("Invalid type");
        errors.add("cannot be cast to");
        errors.add("Duplicated inline view column");

        // functions
        errors.add("No matching function with signature");
        errors.add("Invalid number format");
        errors.add("group_concat requires");
        errors.add("function's argument should be");
        errors.add("requires a numeric parameter");
        errors.add("out of bounds");
        errors.add("function do not support");
        errors.add("parameter must be");
        errors.add("Not supported input arguments types");
        errors.add("No matching function with signature");
        errors.add("function");
        errors.add("Invalid");
        errors.add("Incorrect");

        // regex

        // To avoid bugs
        if (DorisBugs.bug19370) {
            errors.add("failed to initialize storage");
        }
        if (DorisBugs.bug19374) {
            errors.add("the size of the result sets mismatch");
        }
        if (DorisBugs.bug19611) {
            errors.add("Duplicated inline view column alias");
        }

        return errors;
    }

    public static void addExpressionErrors(ExpectedErrors errors) {
        errors.addAll(getExpressionErrors());
    }

    public static List<String> getInsertErrors() {
        ArrayList<String> errors = new ArrayList<>();

        errors.add("Insert has filtered data in strict mode");
        errors.add("Only value columns of unique table could be updated");
        errors.add("Only unique olap table could be updated");
        errors.add("Number out of range");

        return errors;
    }

    public static void addInsertErrors(ExpectedErrors errors) {
        errors.addAll(getInsertErrors());
    }

}
