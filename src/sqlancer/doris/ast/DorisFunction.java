package sqlancer.doris.ast;

import java.util.List;

import sqlancer.common.ast.newast.NewFunctionNode;

public class DorisFunction<F> extends NewFunctionNode<DorisExpression, F> implements DorisExpression {
    public DorisFunction(List<DorisExpression> args, F func) {
        super(args, func);
    }
}
