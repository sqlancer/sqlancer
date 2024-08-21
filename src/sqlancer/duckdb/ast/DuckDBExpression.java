package sqlancer.duckdb.ast;

import sqlancer.common.ast.newast.Expression;
import sqlancer.duckdb.DuckDBSchema.DuckDBColumn;

public interface DuckDBExpression extends Expression<DuckDBColumn> {
}
