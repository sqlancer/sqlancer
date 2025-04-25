package sqlancer.oxla.ast;

import sqlancer.common.ast.newast.NewFunctionNode;

import java.util.List;

public class OxlaFunction<F> extends NewFunctionNode<OxlaExpression, F>
        implements OxlaExpression {
    public OxlaFunction(List<OxlaExpression> args, F func) {
        super(args, func);
    }
}
