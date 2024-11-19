package sqlancer.influxdb.ast;

import java.util.List;
import java.util.stream.Collectors;

public class InfluxDBGroupBy {

    private List<String> fields;
    private String timeInterval;

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