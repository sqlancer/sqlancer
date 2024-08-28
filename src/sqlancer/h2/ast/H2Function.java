package sqlancer.h2.ast;

import java.util.List;

import sqlancer.common.ast.newast.NewFunctionNode;

public class H2Function<F> extends NewFunctionNode<H2Expression, F> implements H2Expression {
    public H2Function(List<H2Expression> args, F func) {
        super(args, func);
    }
}
