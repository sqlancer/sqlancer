package sqlancer.influxdb.ast;

import sqlancer.common.ast.BinaryOperatorNode;
import sqlancer.common.ast.newast.NewUnaryPostfixOperatorNode;

public class InfluxDBUnaryPostfixOperator extends NewUnaryPostfixOperatorNode<InfluxDBExpression>
        implements InfluxDBExpression {

    public InfluxDBUnaryPostfixOperator(InfluxDBExpression expr, BinaryOperatorNode.Operator op) {
        super(expr, op);
    }
    

}