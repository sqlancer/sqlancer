package sqlancer.sqlite3;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import sqlancer.common.query.ExpectedErrors;

class TestSQLite3Errors {

    @Test
    void foreignKeyConstraintIsExpectedForInsertAndUpdate() {
        ExpectedErrors errors = new ExpectedErrors();
        SQLite3Errors.addInsertUpdateErrors(errors);

        assertTrue(errors.errorIsExpected("[SQLITE_CONSTRAINT_FOREIGNKEY] FOREIGN KEY constraint failed"));
    }
}
