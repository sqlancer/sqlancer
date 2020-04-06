package sqlancer.ast;

import sqlancer.ast.newast.Node;
import sqlancer.clickhouse.ast.ClickhouseExpression;

public class ClickhouseSelect extends SelectBase<Node<ClickhouseExpression>> implements Node<ClickhouseExpression> {

}
