package sqlancer.influxdb.ast;

import sqlancer.influxdb.ast.InfluxDBExpression;

public class InfluxDBFill implements InfluxDBExpression {

    public enum FillOption {
        NULL, NONE, PREVIOUS, LINEAR, NUMERIC
    }

    private FillOption fillOption;
    private Double numericValue;  // Used if the fill option is NUMERIC

    public InfluxDBFill(FillOption fillOption) {
        this.fillOption = fillOption;
    }

    public InfluxDBFill(double numericValue) {
        this.fillOption = FillOption.NUMERIC;
        this.numericValue = numericValue;
    }

    public FillOption getFillOption() {
        return fillOption;
    }

    public void setFillOption(FillOption fillOption) {
        this.fillOption = fillOption;
    }

    public Double getNumericValue() {
        return numericValue;
    }

    public void setNumericValue(Double numericValue) {
        this.numericValue = numericValue;
    }

    public String toFillClause() {
        switch (fillOption) {
            case NULL:
                return "FILL(null)";
            case NONE:
                return "FILL(none)";
            case PREVIOUS:
                return "FILL(previous)";
            case LINEAR:
                return "FILL(linear)";
            case NUMERIC:
                return "FILL(" + numericValue + ")";
            default:
                throw new IllegalArgumentException("Unknown fill option: " + fillOption);
        }
    }

    @Override
    public String toString() {
        return toFillClause();
    }
}