package sqlancer.doris;

import sqlancer.common.query.ExpectedErrors;

public final class DorisErrors {

    private DorisErrors() {
    }

    public static void addExpressionErrors(ExpectedErrors errors) {
        // SQL syntax error
        errors.add("Syntax error");
        errors.add("Please check your sql, we meet an error when parsing");
        errors.add("but returns type");
        errors.add("is not a number");

        // Not in line with Doris' logic
        errors.add("Unexpected exception: null");
        errors.add("Cross join can't be used with ON clause");
        errors.add("BetweenPredicate needs to be rewritten into a CompoundPredicate");
        errors.add("can't be assigned to some PlanNode");
        errors.add("can not cast from origin type");
        errors.add("not produced by aggregation output");
        errors.add("cannot combine"); // cannot combine SELECT DISTINCT with aggregate functions or GROUP BY

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

        // regex

        // To avoid bugs
        if (DorisBugs.bug19370) {
            errors.add("failed to initialize storage");
        }
        if (DorisBugs.bug19374) {
            errors.add("the size of the result sets mismatch");
        }
    }

    public static void addInsertErrors(ExpectedErrors errors) {
        errors.add("Insert has filtered data in strict mode");
        errors.add("Only value columns of unique table could be updated");
        errors.add("Only unique olap table could be updated");
        errors.add("Number out of range");
    }

}
