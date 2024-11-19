package sqlancer.influxdb.ast;

import sqlancer.influxdb.ast.InfluxDBExpression;

public class InfluxDBLimit implements InfluxDBExpression {
    
    private int limit;
    private int offset;

    public InfluxDBLimit(int limit) {
        this.limit = limit;
        this.offset = 0;  // Default value when no offset is provided
    }

    public InfluxDBLimit(int limit, int offset) {
        this.limit = limit;
        this.offset = offset;
    }

    public int getLimit() {
        return limit;
    }

    public void setLimit(int limit) {
        this.limit = limit;
    }

    public int getOffset() {
        return offset;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }


    public String toLimitClause() {
        if (offset > 0) {
            return "LIMIT " + limit + " OFFSET " + offset;
        }
        return "LIMIT " + limit;
    }

    @Override
    public String toString() {
        return toLimitClause();
    }


}