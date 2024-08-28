package sqlancer.duckdb.ast;

import sqlancer.common.ast.newast.ColumnReferenceNode;
import sqlancer.duckdb.DuckDBSchema;

public class DuckDBColumnReference extends ColumnReferenceNode<DuckDBExpression, DuckDBSchema.DuckDBColumn>
        implements DuckDBExpression {
    public DuckDBColumnReference(DuckDBSchema.DuckDBColumn column) {
        super(column);
    }

}
