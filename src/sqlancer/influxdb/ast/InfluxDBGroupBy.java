package sqlancer.influxdb.ast;

import sqlancer.common.ast.newast.NewGroupByTerm;

public class InfluxDBGroupBy extends NewGroupByTerm<InfluxDBExpression> implements InfluxDBExpression {

    public InfluxDBGroupBy(InfluxDBExpression expr) {
        super(expr);
    }

    @Override
    public String asString() {
        return expr.toString();
    }
}