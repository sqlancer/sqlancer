package sqlancer.mariadb;

import sqlancer.common.query.ExpectedErrors;

public final class MariaDBErrors {

    private MariaDBErrors() {
    }

    public static void addInsertErrors(ExpectedErrors errors) {
        errors.add("Out of range");
        errors.add("Duplicate entry"); // violates UNIQUE constraint
        errors.add("cannot be null"); // violates NOT NULL constraint
        errors.add("Incorrect integer value"); // e.g., insert TEXT into an int value
        errors.add("Data truncated for column"); // int + plus string into int
        errors.add("doesn't have a default value"); // no default value
        errors.add("The value specified for generated column"); // trying to insert into a generated column
        errors.add("Incorrect double value");
        errors.add("Incorrect string value");
    }

}
