package sqlancer.questdb.ast;

import sqlancer.common.ast.newast.TableReferenceNode;
import sqlancer.questdb.QuestDBSchema;

public class QuestDBTableReference extends TableReferenceNode<QuestDBExpression, QuestDBSchema.QuestDBTable>
        implements QuestDBExpression {
    public QuestDBTableReference(QuestDBSchema.QuestDBTable table) {
        super(table);
    }
}
