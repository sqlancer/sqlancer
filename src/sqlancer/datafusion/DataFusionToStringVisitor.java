package sqlancer.datafusion;

import java.util.List;

import sqlancer.common.ast.SelectBase;
import sqlancer.common.ast.newast.NewToStringVisitor;
import sqlancer.datafusion.ast.DataFusionConstant;
import sqlancer.datafusion.ast.DataFusionExpression;
import sqlancer.datafusion.ast.DataFusionJoin;
import sqlancer.datafusion.ast.DataFusionSelect;

public class DataFusionToStringVisitor extends NewToStringVisitor<DataFusionExpression> {

    public static String asString(DataFusionExpression expr) {
        DataFusionToStringVisitor visitor = new DataFusionToStringVisitor();
        visitor.visit(expr);
        return visitor.get();
    }

    public static String asString(List<DataFusionExpression> exprs) {
        DataFusionToStringVisitor visitor = new DataFusionToStringVisitor();
        visitor.visit(exprs);
        return visitor.get();
    }

    @Override
    public void visitSpecific(DataFusionExpression expr) {
        if (expr instanceof DataFusionConstant) {
            visit((DataFusionConstant) expr);
        } else if (expr instanceof DataFusionSelect) {
            visit((DataFusionSelect) expr);
        } else if (expr instanceof DataFusionJoin) {
            visit((DataFusionJoin) expr);
        } else {
            throw new AssertionError(expr.getClass());
        }
    }

    private void visit(DataFusionJoin join) {
        visit((DataFusionExpression) join.getLeftTable());
        sb.append(" ");
        sb.append(join.getJoinType());
        sb.append(" ");

        sb.append(" JOIN ");
        visit((DataFusionExpression) join.getRightTable());
        if (join.getOnCondition() != null) {
            sb.append(" ON ");
            visit(join.getOnCondition());
        }
    }

    private void visit(DataFusionConstant constant) {
        sb.append(constant.toString());
    }

    private void visit(DataFusionSelect select) {
        visitSelect(select);
    }

    @Override
    protected void visitSelectColumns(SelectBase<DataFusionExpression> select) {
        DataFusionSelect dfSelect = (DataFusionSelect) select;
        if (dfSelect.fetchColumnsString.isPresent()) {
            sb.append(dfSelect.fetchColumnsString.get());
        } else {
            visit(select.getFetchColumns());
        }
    }
}
