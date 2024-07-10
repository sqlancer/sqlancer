package sqlancer.timescaledb.gen;

import java.util.ArrayList;
import java.util.List;

import sqlancer.common.query.ExpectedErrors;

public final class TimescaleDBCommon {
    private TimescaleDBCommon() {

    }

    public static List<String> getTimescaleDBErrors() {
        return new ArrayList<>();
    }

    public static void addTimescaleDBErrors(ExpectedErrors errors) {
        errors.addAll(getTimescaleDBErrors());
    }
}
