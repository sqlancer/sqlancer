package sqlancer.influxdb.ast;

import sqlancer.common.ast.newast.ColumnReferenceNode;
import sqlancer.influxdb.InfluxDBSchema;

public class InfluxDBTimeReference extends ColumnReferenceNode<InfluxDBExpression, InfluxDBSchema.InfluxDBColumn>
        implements InfluxDBExpression {
    
    public InfluxDBTimeReference(InfluxDBSchema.InfluxDBColumn column) {
        super(column);
    }
}