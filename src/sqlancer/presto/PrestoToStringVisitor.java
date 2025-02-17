package sqlancer.presto;

import java.util.List;

import sqlancer.common.ast.newast.NewToStringVisitor;
import sqlancer.presto.ast.PrestoAtTimeZoneOperator;
import sqlancer.presto.ast.PrestoCastFunction;
import sqlancer.presto.ast.PrestoConstant;
import sqlancer.presto.ast.PrestoExpression;
import sqlancer.presto.ast.PrestoFunctionWithoutParenthesis;
import sqlancer.presto.ast.PrestoJoin;
import sqlancer.presto.ast.PrestoMultiValuedComparison;
import sqlancer.presto.ast.PrestoMultiValuedComparisonType;
import sqlancer.presto.ast.PrestoQuantifiedComparison;
import sqlancer.presto.ast.PrestoSelect;

public class PrestoToStringVisitor extends NewToStringVisitor<PrestoExpression> {

    public static String asString(PrestoExpression expr) {
        PrestoToStringVisitor visitor = new PrestoToStringVisitor();
        visitor.visit(expr);
        return visitor.get();
    }

    @Override
    public void visitSpecific(PrestoExpression expr) {
        if (expr instanceof PrestoConstant) {
            visit((PrestoConstant) expr);
        } else if (expr instanceof PrestoSelect) {
            visit((PrestoSelect) expr);
        } else if (expr instanceof PrestoJoin) {
            visit((PrestoJoin) expr);
        } else if (expr instanceof PrestoCastFunction) {
            visit((PrestoCastFunction) expr);
        } else if (expr instanceof PrestoFunctionWithoutParenthesis) {
            visit((PrestoFunctionWithoutParenthesis) expr);
        } else if (expr instanceof PrestoAtTimeZoneOperator) {
            visit((PrestoAtTimeZoneOperator) expr);
        } else if (expr instanceof PrestoMultiValuedComparison) {
            visit((PrestoMultiValuedComparison) expr);
        } else if (expr instanceof PrestoQuantifiedComparison) {
            visit((PrestoQuantifiedComparison) expr);
        } else {
            throw new AssertionError(expr.getClass());
        }
    }

    private void visit(PrestoJoin join) {
        visit((PrestoExpression) join.getLeftTable());
        sb.append(" ");
        sb.append(join.getJoinType());
        sb.append(" ");
        if (join.getOuterType() != null) {
            sb.append(join.getOuterType());
        }
        sb.append(" JOIN ");
        visit((PrestoExpression) join.getRightTable());
        if (join.getOnCondition() != null) {
            sb.append(" ON ");
            visit(join.getOnCondition());
        }
    }

    private void visit(PrestoConstant constant) {
        sb.append(constant.toString());
    }

    private void visit(PrestoAtTimeZoneOperator timeZoneOperator) {
        visit(timeZoneOperator.getExpr());
        sb.append(" AT TIME ZONE ");
        sb.append(timeZoneOperator.getTimeZone());
    }

    private void visit(PrestoFunctionWithoutParenthesis prestoFunctionWithoutParenthesis) {
        sb.append(prestoFunctionWithoutParenthesis.getExpr());
    }

    private void visit(PrestoSelect select) {
        visitSelect(select);
    }

    public void visit(PrestoCastFunction cast) {
        sb.append("CAST((");
        visit(cast.getExpr());
        sb.append(") AS ");
        sb.append(cast.getType().toString());
        sb.append(")");
    }

    public void visit(PrestoMultiValuedComparison comp) {
        visitComparison(comp.getLeft(), comp.getOp().getStringRepresentation(), comp.getType(), null, comp.getRight(),
                " (VALUES |)");
    }

    public void visit(PrestoQuantifiedComparison comp) {
        visitComparison(comp.getLeft(), comp.getOp().getStringRepresentation(), comp.getType(), comp.getRight(), null,
                " ( | ) ");
    }

    private void visitComparison(PrestoExpression left, String operator, PrestoMultiValuedComparisonType type,
            PrestoExpression singleRight, List<PrestoExpression> listRight, String rightWrapper) {
        sb.append("(");
        visit(left);
        sb.append(" ");
        sb.append(operator);
        sb.append(" ");
        sb.append(type);
        sb.append(rightWrapper.split("\\|")[0]);

        if (listRight != null) {
            visit(listRight);
        } else {
            visit(singleRight);
        }

        sb.append(rightWrapper.split("\\|")[1]);
        sb.append(")");
    }
}
