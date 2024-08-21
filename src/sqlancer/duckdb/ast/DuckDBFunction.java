package sqlancer.duckdb.ast;

import java.util.List;

import sqlancer.common.ast.newast.NewFunctionNode;

public class DuckDBFunction<F> extends NewFunctionNode<DuckDBExpression, F> implements DuckDBExpression {
    public DuckDBFunction(List<DuckDBExpression> args, F func) {
        super(args, func);
    }
}
