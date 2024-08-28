package sqlancer.duckdb.ast;

import sqlancer.common.ast.newast.TableReferenceNode;
import sqlancer.duckdb.DuckDBSchema;

public class DuckDBTableReference extends TableReferenceNode<DuckDBExpression, DuckDBSchema.DuckDBTable>
        implements DuckDBExpression {
    public DuckDBTableReference(DuckDBSchema.DuckDBTable table) {
        super(table);
    }
}
