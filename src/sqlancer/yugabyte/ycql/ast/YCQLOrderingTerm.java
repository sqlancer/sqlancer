package sqlancer.yugabyte.ycql.ast;

import sqlancer.common.ast.newast.NewOrderingTerm;
import sqlancer.duckdb.ast.DuckDBExpression;

public class DuckDBOrderingTerm extends NewOrderingTerm<DuckDBExpression> implements DuckDBExpression {
    public DuckDBOrderingTerm(DuckDBExpression expr, Ordering ordering) {
        super(expr, ordering);
    }
}
