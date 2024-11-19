package sqlancer.influxdb.ast;

import java.util.List;
import sqlancer.common.ast.newast.NewFunctionNode;

public class InfluxDBFunction<F> extends NewFunctionNode<InfluxDBExpression, F> implements InfluxDBExpression {

    public InfluxDBFunction(List<InfluxDBExpression> args, F func) {
        super(args, func);
    }
}