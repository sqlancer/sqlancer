package sqlancer.hive.ast;

import sqlancer.common.ast.newast.ColumnReferenceNode;
import sqlancer.hive.HiveSchema.HiveColumn;

public class HiveColumnReference extends ColumnReferenceNode<HiveExpression, HiveColumn> implements HiveExpression {

    public HiveColumnReference(HiveColumn column) {
        super(column);
    }
}
