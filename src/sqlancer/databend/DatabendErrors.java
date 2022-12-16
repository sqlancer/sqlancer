package sqlancer.databend;

import sqlancer.common.query.ExpectedErrors;

public final class DatabendErrors {

    private DatabendErrors() {
    }

    public static void addExpressionErrors(ExpectedErrors errors) {
        errors.add("Division by zero");
        errors.add("/ by zero");
        errors.add("ORDER BY position");
        errors.add("GROUP BY position");
        if (DatabendBugs.bug9162) {
            errors.add("downcast column error");
        }
        if (DatabendBugs.bug9018) {
            errors.add("index out of bounds");
        }
        if (DatabendBugs.bug9163) {
            errors.add("validity must be equal to the array's length");
        }
        if (DatabendBugs.bug9224) {
            errors.add("Can't cast column from nullable data into non-nullable type");
        }
        if (DatabendBugs.bug9234) {
            errors.add("called `Option::unwrap()` on a `None` value");
        }
        if (DatabendBugs.bug9264) {
            errors.add("assertion failed: offset + length <= self.length");
        }
        // errors.add("validity's length must be equal"); // bug

        /*
         * TODO column为not null 时，注意default不能为null DROP DATABASE IF EXISTS databend2; CREATE DATABASE databend2; USE
         * databend2; CREATE TABLE t0(c0VARCHAR VARCHAR NULL, c1VARCHAR VARCHAR NULL, c2FLOAT FLOAT NOT NULL
         * DEFAULT(NULL)); CREATE TABLE t1(c0INT BIGINT NULL); INSERT INTO t0(c1varchar, c0varchar) VALUES
         * ('067596','19'), ('', '87');
         */
        errors.add("Can't cast column from null into non-nullable type");
    }

    public static void addInsertErrors(ExpectedErrors errors) {
        errors.add("Division by zero");
        errors.add("/ by zero");
        errors.add("Can't cast column from null into non-nullable type");
    }

    public static void addGroupByErrors(ExpectedErrors errors) {
        errors.add("Division by zero");
        errors.add("/ by zero");
        errors.add("Can't cast column from null into non-nullable type");
        errors.add("GROUP BY position");
    }

}
