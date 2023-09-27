package sqlancer.presto.ast;

import sqlancer.common.ast.newast.TableReferenceNode;
import sqlancer.presto.PrestoSchema;

public class PrestoTableReference extends TableReferenceNode<PrestoExpression, PrestoSchema.PrestoTable> {

    public PrestoTableReference(PrestoSchema.PrestoTable table) {
        super(table);
    }
}
