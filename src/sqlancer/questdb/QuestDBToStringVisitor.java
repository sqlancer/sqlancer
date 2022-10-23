package sqlancer.questdb;

import sqlancer.common.ast.newast.NewToStringVisitor;
import sqlancer.common.ast.newast.Node;
import sqlancer.questdb.ast.QuestDBConstant;
import sqlancer.questdb.ast.QuestDBExpression;

public class QuestDBToStringVisitor extends NewToStringVisitor<QuestDBExpression> {

    @Override
    public void visitSpecific(Node<QuestDBExpression> expr) {
        if (expr instanceof QuestDBConstant) {
            visit((QuestDBConstant) expr);
        } else { // TODO: maybe implement QuestDBSelect & Join
            throw new AssertionError("Unknown class: " + expr.getClass());
        }
    }

    private void visit(QuestDBConstant constant) {
        sb.append(constant.toString());
    }

    public static String asString(Node<QuestDBExpression> expr) {
        QuestDBToStringVisitor visitor = new QuestDBToStringVisitor();
        visitor.visit(expr);
        return visitor.get();
    }
}
