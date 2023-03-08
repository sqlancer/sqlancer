package sqlancer.timescaledb.gen;

import sqlancer.common.query.ExpectedErrors;

public final class TimescaleDBCommon {
    private TimescaleDBCommon() {

    }

    public static void addTimescaleDBErrors(ExpectedErrors errors) {
        errors.add("Error updating TimescaleDB when using a third-party PostgreSQL administration tool");
        errors.add("Log error: could not access file \"timescaledb\"");
        errors.add("ERROR: could not access file \"timescaledb-\\<version>\": No such file or directory");
        errors.add("Scheduled jobs stop running");
        errors.add("Failed to start a background worker");
    }
}
