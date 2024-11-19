package sqlancer.influxdb.ast;

import sqlancer.common.ast.newast.ColumnReferenceNode;
import sqlancer.influxdb.InfluxDBSchema.InfluxDBColumn;

public class InfluxDBTagReference extends ColumnReferenceNode<InfluxDBExpression, InfluxDBColumn>
        implements InfluxDBExpression {
     public InfluxDBTagReference(InfluxDBColumn column) {
        super(column);
    }
}