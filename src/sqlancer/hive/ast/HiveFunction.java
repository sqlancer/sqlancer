package sqlancer.hive.ast;

import java.util.List;

import sqlancer.common.ast.newast.NewFunctionNode;

public class HiveFunction<F> extends NewFunctionNode<HiveExpression, F> implements HiveExpression {

    public HiveFunction(List<HiveExpression> args, F func) {
        super(args, func);
    }
    
}
