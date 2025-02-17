package sqlancer.doris.visitor;

import sqlancer.Randomly;
import sqlancer.common.ast.newast.NewToStringVisitor;
import sqlancer.doris.ast.DorisCaseOperation;
import sqlancer.doris.ast.DorisCastOperation;
import sqlancer.doris.ast.DorisConstant;
import sqlancer.doris.ast.DorisExpression;
import sqlancer.doris.ast.DorisFunctionOperation;
import sqlancer.doris.ast.DorisJoin;
import sqlancer.doris.ast.DorisSelect;

public class DorisToStringVisitor extends NewToStringVisitor<DorisExpression> {

    @Override
    public void visitSpecific(DorisExpression expr) {
        if (expr instanceof DorisConstant) {
            visit((DorisConstant) expr);
        } else if (expr instanceof DorisSelect) {
            visit((DorisSelect) expr);
        } else if (expr instanceof DorisJoin) {
            visit((DorisJoin) expr);
        } else if (expr instanceof DorisCastOperation) {
            visit((DorisCastOperation) expr);
        } else if (expr instanceof DorisCaseOperation) {
            visit((DorisCaseOperation) expr);
        } else if (expr instanceof DorisFunctionOperation) {
            visit((DorisFunctionOperation) expr);
        } else {
            throw new AssertionError(expr.getClass());
        }
    }

    private void visit(DorisJoin join) {
        sb.append(" ");
        visit((DorisExpression) join.getLeftTable());
        sb.append(" ");
        switch (join.getJoinType()) {
        case INNER:
            if (Randomly.getBoolean()) {
                sb.append("INNER ");
            } else {
                sb.append("CROSS ");
            }
            sb.append("JOIN ");
            break;
        case LEFT:
            sb.append("LEFT ");
            if (Randomly.getBoolean()) {
                sb.append(" OUTER ");
            }
            sb.append("JOIN ");
            break;
        case RIGHT:
            sb.append("RIGHT ");
            if (Randomly.getBoolean()) {
                sb.append(" OUTER ");
            }
            sb.append("JOIN ");
            break;
        case STRAIGHT:
            sb.append("STRAIGHT_JOIN ");
            break;
        default:
            throw new AssertionError();
        }
        visit((DorisExpression) join.getRightTable());
        sb.append(" ");
        if (join.getOnCondition() != null) {
            sb.append("ON ");
            visit(join.getOnCondition());
        }
    }

    private void visit(DorisConstant constant) {
        sb.append(constant.toString());
    }

    private void visit(DorisCastOperation castExpr) {
        sb.append("CAST(");
        visit(castExpr.getExpr());
        sb.append(" AS ");
        sb.append(castExpr.getType().toString());
        sb.append(") ");
    }

    private void visit(DorisFunctionOperation func) {
        sb.append(func.getFunction().getFunctionName());
        sb.append("(");

        if (func.getArgs() != null) {
            for (int i = 0; i < func.getArgs().size(); i++) {
                visit(func.getArgs().get(i));
                if (i != func.getArgs().size() - 1) {
                    sb.append(",");
                }
            }
        }
        sb.append(") ");
    }

    private void visit(DorisCaseOperation cases) {
        sb.append("CASE ");
        visit(cases.getExpr());
        sb.append(" ");
        for (int i = 0; i < cases.getConditions().size(); i++) {
            DorisExpression predicate = cases.getConditions().get(i);
            DorisExpression then = cases.getThenClauses().get(i);
            sb.append(" WHEN ");
            visit(predicate);
            sb.append(" THEN ");
            visit(then);
            sb.append(" ");
        }
        if (cases.getElseClause() != null) {
            sb.append("ELSE ");
            visit(cases.getElseClause());
            sb.append(" ");
        }
        sb.append("END ");
    }

    private void visit(DorisSelect select) {
        visitSelect(select);
    }

    public static String asString(DorisExpression expr) {
        DorisToStringVisitor visitor = new DorisToStringVisitor();
        visitor.visit(expr);
        return visitor.get();
    }
}
