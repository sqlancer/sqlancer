package influxdb.ast;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Represents a GROUP BY clause in an InfluxDB query.
 */
public class InfluxDBGroupBy {

    private List<String> fields;
    private String timeInterval;

    /**
     * Constructs an InfluxDBGroupBy with the specified fields and time interval.
     * 
     * @param fields The list of fields to group by.
     * @param timeInterval The time interval for grouping.
     */
    public InfluxDBGroupBy(List<String> fields, String timeInterval) {
        this.fields = fields;
        this.timeInterval = timeInterval;
    }

    public List<String> getFields() {
        return fields;
    }

    public void setFields(List<String> fields) {
        this.fields = fields;
    }

    public String getTimeInterval() {
        return timeInterval;
    }

    public void setTimeInterval(String timeInterval) {
        this.timeInterval = timeInterval;
    }

    /**
     * Converts the InfluxDBGroupBy instance to a GROUP BY clause string.
     * 
     * @return The GROUP BY clause string.
     */
    public String toGroupByClause() {
        String fieldsPart = fields.stream().collect(Collectors.joining(", "));
        if (timeInterval != null && !timeInterval.isEmpty()) {
            if (!fields.isEmpty()) {
                fieldsPart += ", ";
            }
            fieldsPart += "time(" + timeInterval + ")";
        }
        return "GROUP BY " + fieldsPart;
    }

    @Override
    public String toString() {
        return toGroupByClause();
    }
}