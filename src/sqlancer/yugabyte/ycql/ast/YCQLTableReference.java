package sqlancer.yugabyte.ycql.ast;

import sqlancer.common.ast.newast.TableReferenceNode;
import sqlancer.questdb.QuestDBSchema;
import sqlancer.questdb.ast.QuestDBExpression;

public class QuestDBTableReference extends TableReferenceNode<QuestDBExpression, QuestDBSchema.QuestDBTable>
        implements QuestDBExpression {
    public QuestDBTableReference(QuestDBSchema.QuestDBTable table) {
        super(table);
    }
}
