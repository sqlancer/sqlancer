package sqlancer.h2.ast;

import sqlancer.common.ast.newast.ColumnReferenceNode;
import sqlancer.h2.H2Schema;

public class H2ColumnReference extends ColumnReferenceNode<H2Expression, H2Schema.H2Column> implements H2Expression {
    public H2ColumnReference(H2Schema.H2Column column) {
        super(column);
    }

}
