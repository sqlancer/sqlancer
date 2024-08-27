package sqlancer.yugabyte.ycql.ast;

import java.util.List;

import sqlancer.common.ast.newast.NewFunctionNode;
import sqlancer.duckdb.ast.DuckDBExpression;

public class DuckDBFunction<F> extends NewFunctionNode<DuckDBExpression, F> implements DuckDBExpression {
    public DuckDBFunction(List<DuckDBExpression> args, F func) {
        super(args, func);
    }
}
