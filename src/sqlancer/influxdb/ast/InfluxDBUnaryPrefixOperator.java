package sqlancer.influxdb.ast;

import sqlancer.common.ast.BinaryOperatorNode;
import sqlancer.common.ast.newast.NewUnaryPrefixOperatorNode;

public class InfluxDBUnaryPrefixOperator extends NewUnaryPrefixOperatorNode<InfluxDBExpression>
        implements InfluxDBExpression {

    public InfluxDBUnaryPrefixOperator(InfluxDBExpression expr, BinaryOperatorNode.Operator operator) {
        super(expr, operator);
    }
}