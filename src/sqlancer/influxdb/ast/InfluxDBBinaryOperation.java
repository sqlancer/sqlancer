package sqlancer.influxdb.ast;

import sqlancer.common.ast.newast.NewBinaryOperatorNode;
import sqlancer.influxdb.gen.InfluxDBExpressionGenerator;

public class InfluxDBBinaryOperation extends NewBinaryOperatorNode<InfluxDBExpression> implements InfluxDBExpression {
    public InfluxDBBinaryOperation(InfluxDBExpression left, InfluxDBExpression right, InfluxDBExpressionGenerator.InfluxDBBinaryLogicalOperator op) {
        super(left, right, op);
    }
}