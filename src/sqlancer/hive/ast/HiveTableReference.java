package sqlancer.hive.ast;

import sqlancer.common.ast.newast.TableReferenceNode;
import sqlancer.hive.HiveSchema;

public class HiveTableReference extends TableReferenceNode<HiveExpression, HiveSchema.HiveTable> 
        implements HiveExpression {

    public HiveTableReference(HiveSchema.HiveTable table) {
        super(table);
    }

}