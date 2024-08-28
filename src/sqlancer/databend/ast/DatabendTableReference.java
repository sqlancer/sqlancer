package sqlancer.databend.ast;

import sqlancer.common.ast.newast.TableReferenceNode;
import sqlancer.databend.DatabendSchema;

public class DatabendTableReference extends TableReferenceNode<DatabendExpression, DatabendSchema.DatabendTable>
        implements DatabendExpression {
    public DatabendTableReference(DatabendSchema.DatabendTable table) {
        super(table);
    }
}
