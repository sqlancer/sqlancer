package sqlancer.doris.ast;

import sqlancer.common.ast.newast.TableReferenceNode;
import sqlancer.doris.DorisSchema;

public class DorisTableReference extends TableReferenceNode<DorisExpression, DorisSchema.DorisTable>
        implements DorisExpression {
    public DorisTableReference(DorisSchema.DorisTable table) {
        super(table);
    }
}
