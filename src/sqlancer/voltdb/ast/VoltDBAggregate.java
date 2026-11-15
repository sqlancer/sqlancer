package sqlancer.voltdb.ast;

import java.util.List;

import sqlancer.common.ast.newast.NewFunctionNode;

// An aggregate function call; COUNT(*) has no arguments.
public class VoltDBAggregate extends NewFunctionNode<VoltDBExpression, VoltDBAggregate.VoltDBAggregateFunction>
        implements VoltDBExpression {

    public enum VoltDBAggregateFunction {
        COUNT, SUM, MIN, MAX
    }

    public VoltDBAggregate(List<VoltDBExpression> args, VoltDBAggregateFunction func) {
        super(args, func);
    }

}
