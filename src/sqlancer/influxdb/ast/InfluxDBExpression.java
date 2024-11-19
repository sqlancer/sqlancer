package sqlancer.influxdb.ast;

import sqlancer.common.ast.newast.Expression;
import sqlancer.influxdb.InfluxDBSchema.InfluxDBColumn;

public interface InfluxDBExpression extends Expression<InfluxDBColumn> {
}