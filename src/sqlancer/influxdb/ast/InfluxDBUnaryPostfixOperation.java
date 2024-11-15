package sqlancer.influxdb.ast;

import sqlancer.common.ast.BinaryOperatorNode;
import sqlancer.common.ast.newast.NewUnaryPostfixOperatorNode;

public class InfluxDBUnaryPostfixOperation extends NewUnaryPostfixOperatorNode<InfluxDBExpression>
        implements InfluxDBExpression {

    public InfluxDBUnaryPostfixOperation(InfluxDBExpression expr, BinaryOperatorNode op) {
        super(expr, BinaryOperatorNode);
    }
}