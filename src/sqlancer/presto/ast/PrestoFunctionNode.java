package sqlancer.presto.ast;

import java.util.List;

import sqlancer.common.ast.newast.NewFunctionNode;

public class PrestoFunctionNode<F> extends NewFunctionNode<PrestoExpression, F> implements PrestoExpression {
    public PrestoFunctionNode(List<PrestoExpression> args, F func) {
        super(args, func);
    }
}
