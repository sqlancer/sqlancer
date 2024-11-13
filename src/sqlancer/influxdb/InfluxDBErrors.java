package sqlancer.influxdb;

import java.util.ArrayList;
import java.util.List;
import sqlancer.common.query.ExpectedErrors;

public final class InfluxDBErrors {
    private InfluxDBErrors() {
    }

    public static List<String> getExpressionErrors() {
        ArrayList<String> errors = new ArrayList<>();

        // InfluxDB specific errors
        errors.add("unexpected keyword");
        errors.add("invalid expression");
        errors.add("unsupported operand");
        errors.add("field type conflict");
        errors.add("missing field");
        errors.add("cannot compare type");

        return errors;
    }

    public static void addExpressionErrors(ExpectedErrors errors) {
        errors.addAll(getExpressionErrors());
    }

    public static List<String> getGroupByErrors() {
        ArrayList<String> errors = new ArrayList<>();

        // InfluxDB specific errors
        errors.add("GROUP BY requires a time or tag");
        errors.add("GROUP BY time requires an aggregate function");

        return errors;
    }

    public static void addGroupByErrors(ExpectedErrors errors) {
        errors.addAll(getGroupByErrors());
    }

    public static List<String> getInsertErrors() {
        ArrayList<String> errors = new ArrayList<>();

        // InfluxDB specific errors
        errors.add("unable to parse");
        errors.add("field type conflict");
        errors.add("measurement not found");

        return errors;
    }

    public static void addInsertErrors(ExpectedErrors errors) {
        errors.addAll(getInsertErrors());
    }
}