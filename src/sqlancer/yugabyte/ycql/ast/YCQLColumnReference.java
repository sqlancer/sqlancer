package sqlancer.yugabyte.ycql.ast;

import sqlancer.common.ast.newast.ColumnReferenceNode;
import sqlancer.questdb.QuestDBSchema;
import sqlancer.questdb.ast.QuestDBExpression;

public class QuestDBColumnReference extends ColumnReferenceNode<QuestDBExpression, QuestDBSchema.QuestDBColumn>
        implements QuestDBExpression {
    public QuestDBColumnReference(QuestDBSchema.QuestDBColumn column) {
        super(column);
    }

}
