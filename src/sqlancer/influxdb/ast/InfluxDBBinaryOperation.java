package sqlancer.influxdb.ast;

import sqlancer.common.ast.BinaryOperatorNode.Operator;
import sqlancer.common.ast.newast.NewBinaryOperatorNode;

public class InfluxDBBinaryOperation extends NewBinaryOperatorNode<InfluxDBExpression> implements InfluxDBExpression {
    public InfluxDBBinaryOperation(InfluxDBExpression left, InfluxDBExpression right, Operator op) {
        super(left, right, op);
    }
}