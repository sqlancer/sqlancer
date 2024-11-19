package sqlancer.influxdb.ast;

import sqlancer.common.ast.SelectBase;

public class InfluxDBSelect extends SelectBase<InfluxDBExpression> implements InfluxDBExpression {
    private boolean isDistinct;
    private String measurement; // InfluxDB specific, representing the measurement from which to select

    public InfluxDBSelect() {
        // Initialization if needed
    }


     //@param distinct True to make the selection distinct, false otherwise.

    public void setDistinct(boolean distinct) {
        isDistinct = distinct;
    }


     //@return True if the selection is distinct, false otherwise.

    public boolean isDistinct() {
        return isDistinct;
    }


    //@return The measurement name.
    public String getMeasurement() {
        return measurement;
    }


     //@param measurement The measurement name
    public void setMeasurement(String measurement) {
        this.measurement = measurement;
    }

    // Additional InfluxDB specific methods can be added here
}