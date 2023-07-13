package sqlancer.stonedb;

import sqlancer.common.query.ExpectedErrors;

public final class StoneDBErrors {
    private StoneDBErrors() {
    }

    public static void addExpectedSelectErrors(ExpectedErrors errors) {
        // java.sql.SQLException: Incorrect DATE value: '292269055-12-02'
        errors.add("Incorrect DATE value: ");
    }

}
