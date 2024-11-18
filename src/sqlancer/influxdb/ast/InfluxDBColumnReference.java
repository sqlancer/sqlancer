package sqlancer.influxdb.ast;

import sqlancer.common.ast.newast.ColumnReferenceNode;
import sqlancer.influxdb.InfluxDBSchema;

public class InfluxDBColumnReference extends ColumnReferenceNode<InfluxDBExpression, InfluxDBSchema.InfluxDBColumn>
        implements InfluxDBExpression {

    public InfluxDBColumnReference(InfluxDBSchema.InfluxDBColumn column) {
        super(column);
    }
    
    // Additional methods for InfluxDB specific operations can be added here.
}