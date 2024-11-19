package sqlancer.influxdb.ast;

import sqlancer.common.ast.SelectBase;

public class InfluxDBSelect extends SelectBase<InfluxDBExpression> implements InfluxDBExpression {
    private boolean isDistinct;
    private String measurement; // InfluxDB specific, representing the measurement from which to select

    public InfluxDBSelect() {
        // Initialization if needed
    }


    public void setDistinct(boolean distinct) {
        isDistinct = distinct;
    }


    public boolean isDistinct() {
        return isDistinct;
    }

    public String getMeasurement() {
        return measurement;
    }


    public void setMeasurement(String measurement) {
        this.measurement = measurement;
    }

    // Additional InfluxDB specific methods can be added here
}