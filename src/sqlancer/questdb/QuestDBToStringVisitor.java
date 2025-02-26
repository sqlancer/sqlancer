package sqlancer.questdb;

import sqlancer.common.ast.SelectBase;
import sqlancer.common.ast.newast.NewToStringVisitor;
import sqlancer.questdb.ast.QuestDBConstant;
import sqlancer.questdb.ast.QuestDBExpression;
import sqlancer.questdb.ast.QuestDBSelect;

public class QuestDBToStringVisitor extends NewToStringVisitor<QuestDBExpression> {

    @Override
    public void visitSpecific(QuestDBExpression expr) {
        if (expr instanceof QuestDBConstant) {
            visit((QuestDBConstant) expr);
        } else if (expr instanceof QuestDBSelect) {
            visit((QuestDBSelect) expr);
        } else { // TODO: maybe implement QuestDBJoin
            throw new AssertionError("Unknown class: " + expr.getClass());
        }
    }

    private void visit(QuestDBConstant constant) {
        sb.append(constant.toString());
    }

    private void visit(QuestDBSelect select) {
        visitSelect(select);
    }

    @Override
    protected void visitGroupByClause(SelectBase<QuestDBExpression> select) {
        // Do nothing as QuestDB doesn't support GROUP BY
    }

    @Override
    protected void visitHavingClause(SelectBase<QuestDBExpression> select) {
        // Do nothing as QuestDB doesn't support HAVING
    }

    @Override
    protected void visitOrderByClause(SelectBase<QuestDBExpression> select) {
        // Do nothing as QuestDB doesn't support ORDER BY
    }

    @Override
    protected void visitOffsetClause(SelectBase<QuestDBExpression> select) {
        // Do nothing as QuestDB doesn't support OFFSET
    }

    public static String asString(QuestDBExpression expr) {
        QuestDBToStringVisitor visitor = new QuestDBToStringVisitor();
        visitor.visit(expr);
        return visitor.get();
    }
}
