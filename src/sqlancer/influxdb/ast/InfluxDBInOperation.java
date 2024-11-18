package sqlancer.influxdb.ast;

import java.util.List;
import sqlancer.common.ast.newast.NewInOperatorNode;

public class InfluxDBInOperation extends NewInOperatorNode<InfluxDBExpression> implements InfluxDBExpression {
    public InfluxDBInOperation(InfluxDBExpression left, List<InfluxDBExpression> right, boolean isNegated) {
        super(left, right, isNegated);
    }
}