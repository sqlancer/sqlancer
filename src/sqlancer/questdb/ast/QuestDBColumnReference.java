package sqlancer.questdb.ast;

import sqlancer.common.ast.newast.ColumnReferenceNode;
import sqlancer.questdb.QuestDBSchema;

public class QuestDBColumnReference extends ColumnReferenceNode<QuestDBExpression, QuestDBSchema.QuestDBColumn>
        implements QuestDBExpression {
    public QuestDBColumnReference(QuestDBSchema.QuestDBColumn column) {
        super(column);
    }

}
