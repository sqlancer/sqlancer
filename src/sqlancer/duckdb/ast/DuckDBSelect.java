package sqlancer.duckdb.ast;

import sqlancer.ast.SelectBase;
import sqlancer.ast.newast.Node;

public class DuckDBSelect extends SelectBase<Node<DuckDBExpression>> implements Node<DuckDBExpression> {

}
