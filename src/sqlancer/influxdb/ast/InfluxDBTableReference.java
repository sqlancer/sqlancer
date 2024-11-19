package sqlancer.influxdb.ast;

import sqlancer.common.ast.newast.TableReferenceNode;
import sqlancer.influxdb.InfluxDBSchema;

public class InfluxDBTableReference extends TableReferenceNode<InfluxDBExpression, InfluxDBSchema.InfluxDBTable>
        implements InfluxDBExpression {
    
    public InfluxDBTableReference(InfluxDBSchema.InfluxDBTable table) {
        super(table);
    }
}