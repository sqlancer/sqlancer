package sqlancer.databend.ast;

import java.util.List;

import sqlancer.common.ast.newast.NewFunctionNode;

public class DatabendFunctionOperation<F> extends NewFunctionNode<DatabendExpression, F> implements DatabendExpression {
    public DatabendFunctionOperation(List<DatabendExpression> args, F func) {
        super(args, func);
    }
}
