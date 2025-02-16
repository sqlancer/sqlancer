package sqlancer.yugabyte.ycql;

import sqlancer.common.ast.SelectBase;
import sqlancer.common.ast.newast.NewToStringVisitor;
import sqlancer.questdb.ast.QuestDBExpression;
import sqlancer.yugabyte.ycql.ast.YCQLConstant;
import sqlancer.yugabyte.ycql.ast.YCQLExpression;
import sqlancer.yugabyte.ycql.ast.YCQLSelect;

public class YCQLToStringVisitor extends NewToStringVisitor<YCQLExpression> {

    @Override
    public void visitSpecific(YCQLExpression expr) {
        if (expr instanceof YCQLConstant) {
            visit((YCQLConstant) expr);
        } else if (expr instanceof YCQLSelect) {
            visit((YCQLSelect) expr);
        } else {
            throw new AssertionError(expr.getClass());
        }
    }

    private void visit(YCQLConstant constant) {
        sb.append(constant.toString());
    }

    private void visit(YCQLSelect select) {
        visitSelect(select);
    }

    @Override
    protected void visitGroupByClause(SelectBase<YCQLExpression> select) {
        // Do nothing as YCQL doesn't support GROUP BY
    }

    @Override
    protected void visitHavingClause(SelectBase<YCQLExpression> select) {
        // Do nothing as YCQL doesn't support HAVING
    }

    public static String asString(YCQLExpression expr) {
        YCQLToStringVisitor visitor = new YCQLToStringVisitor();
        visitor.visit(expr);
        return visitor.get();
    }

}
