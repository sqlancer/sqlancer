package sqlancer.h2.ast;

import sqlancer.common.ast.newast.TableReferenceNode;
import sqlancer.h2.H2Schema;

public class H2TableReference extends TableReferenceNode<H2Expression, H2Schema.H2Table> implements H2Expression {
    public H2TableReference(H2Schema.H2Table table) {
        super(table);
    }
}
