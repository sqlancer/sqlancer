package sqlancer.mariadb;

import java.util.ArrayList;
import java.util.List;

import sqlancer.common.query.ExpectedErrors;

public final class MariaDBErrors {

    private MariaDBErrors() {
    }

    public static List<String> getCommonErrors() {
        ArrayList<String> errors = new ArrayList<>();

        errors.add("is out of range");
        // regex
        errors.add("unmatched parentheses");
        errors.add("nothing to repeat at offset");
        errors.add("missing )");
        errors.add("missing terminating ]");
        errors.add("range out of order in character class");
        errors.add("unrecognized character after ");
        errors.add("Got error '(*VERB) not recognized or malformed");
        errors.add("must be followed by");
        errors.add("malformed number or name after");
        errors.add("digit expected after");
        errors.add("Regex error");
        errors.add("Lock wait timeout exceeded");

        return errors;
    }

    public static void addCommonErrors(ExpectedErrors errors) {
        errors.add("is out of range");
        // regex
        errors.add("unmatched parentheses");
        errors.add("nothing to repeat at offset");
        errors.add("missing )");
        errors.add("missing terminating ]");
        errors.add("range out of order in character class");
        errors.add("unrecognized character after ");
        errors.add("Got error '(*VERB) not recognized or malformed");
        errors.add("must be followed by");
        errors.add("malformed number or name after");
        errors.add("digit expected after");
        errors.add("Regex error");
        errors.add("Lock wait timeout exceeded");
    }

    public static List<String> getInsertErrors() {
        ArrayList<String> errors = new ArrayList<>();
        errors.add("Out of range");
        errors.add("Duplicate entry"); // violates UNIQUE constraint
        errors.add("cannot be null"); // violates NOT NULL constraint
        errors.add("Incorrect integer value"); // e.g., insert TEXT into an int value
        errors.add("Data truncated for column"); // int + plus string into int
        errors.add("doesn't have a default value"); // no default value
        errors.add("The value specified for generated column"); // trying to insert into a generated column
        errors.add("Incorrect double value");
        errors.add("Incorrect string value");
        return errors;
    }

    public static void addInsertErrors(ExpectedErrors errors) {
        errors.addAll(getInsertErrors());
    }

}
