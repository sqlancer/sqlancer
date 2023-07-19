package sqlancer.stonedb;

import sqlancer.common.query.ExpectedErrors;

public final class StoneDBErrors {
    private StoneDBErrors() {
    }

    public static void addExpectedExpressionErrors(ExpectedErrors errors) {
        // java.sql.SQLException: Incorrect DATE value: '292269055-12-02'
        errors.add("Incorrect DATE value: ");
        // java.sql.SQLException: Incorrect string value: '\xBC\xE7\xC9\x91\x05R...' for column 'c1' at row 1
        errors.add("Incorrect string value: ");
    }

}
