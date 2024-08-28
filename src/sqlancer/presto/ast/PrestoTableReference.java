package sqlancer.presto.ast;

import sqlancer.common.ast.newast.TableReferenceNode;
import sqlancer.presto.PrestoSchema;

public class PrestoTableReference extends TableReferenceNode<PrestoExpression, PrestoSchema.PrestoTable>
        implements PrestoExpression {

    public PrestoTableReference(PrestoSchema.PrestoTable table) {
        super(table);
    }
}
