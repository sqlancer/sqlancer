package sqlancer.oxla;

import sqlancer.oxla.ast.*;

public class OxlaExpectedValueVisitor {
    private static final OxlaExpectedValueVisitor visitor = new OxlaExpectedValueVisitor();
    private final StringBuilder sb = new StringBuilder();

    public static synchronized String asString(OxlaExpression expr) {
        visitor.reset();
        visitor.visit(expr);
        return visitor.get();
    }

    public synchronized void reset() {
        // Java's STL is stupid; to reset a StringBuilder they recommended to allocate a new one - in performance
        // critical scenarios such as this we get stupid performance hits - reset the string's length to 0 instead.
        sb.setLength(0);
    }

    public void visit(OxlaExpression expr) {
        assert expr != null;
        if (expr instanceof OxlaAlias) {
            visit((OxlaAlias) expr);
        } else if (expr instanceof OxlaBetweenOperator) {
            visit((OxlaBetweenOperator) expr);
        } else if (expr instanceof OxlaBinaryOperation) {
            visit((OxlaBinaryOperation) expr);
        } else if (expr instanceof OxlaCase) {
            visit((OxlaCase) expr);
        } else if (expr instanceof OxlaCast) {
            visit((OxlaCast) expr);
        } else if (expr instanceof OxlaColumnReference) {
            visit((OxlaColumnReference) expr);
        } else if (expr instanceof OxlaConstant) {
            visit((OxlaConstant) expr);
        } else if (expr instanceof OxlaFunction<?>) {
            visit((OxlaFunction<?>) expr);
        } else if (expr instanceof OxlaInOperator) {
            visit((OxlaInOperator) expr);
        } else if (expr instanceof OxlaJoin) {
            visit((OxlaJoin) expr);
        } else if (expr instanceof OxlaOrderingTerm) {
            visit((OxlaOrderingTerm) expr);
        } else if (expr instanceof OxlaPostfixText) {
            visit((OxlaPostfixText) expr);
        } else if (expr instanceof OxlaSelect) {
            visit((OxlaSelect) expr);
        } else if (expr instanceof OxlaTableReference) {
            visit((OxlaTableReference) expr);
        } else if (expr instanceof OxlaTernaryNode) {
            visit((OxlaTernaryNode) expr);
        } else if (expr instanceof OxlaUnaryPostfixOperation) {
            visit((OxlaUnaryPostfixOperation) expr);
        } else if (expr instanceof OxlaUnaryPrefixOperation) {
            visit((OxlaUnaryPrefixOperation) expr);
        } else {
            throw new AssertionError(expr);
        }
    }

    private void visit(OxlaAlias expr) {
        toValue(expr);
        visit(expr.getExpr());
    }

    private void visit(OxlaBetweenOperator expr) {
        toValue(expr);
        visit(expr.getLeft());
        visit(expr.getMiddle());
        visit(expr.getRight());
    }

    private void visit(OxlaBinaryOperation expr) {
        toValue(expr);
        visit(expr.getLeft());
        visit(expr.getRight());
    }

    private void visit(OxlaCase expr) {
        toValue(expr);
        if (expr.getSwitchCondition() != null) {
            toValue(expr.getSwitchCondition());
            visit((expr.getSwitchCondition()));
        }
        for (int index = 0; index < expr.getConditions().size(); ++index) {
            final OxlaExpression condition = expr.getConditions().get(index);
            final OxlaExpression then = expr.getExpressions().get(index);
            toValue(condition);
            visit(condition);
            toValue(then);
            visit(then);
        }
        if (expr.getElseExpr() != null) {
            toValue(expr.getElseExpr());
            visit(expr.getElseExpr());
        }
    }

    private void visit(OxlaCast expr) {
        toValue(expr);
        visit(expr.expression);
    }

    private void visit(OxlaColumnReference expr) {
        toValue(expr);
    }

    private void visit(OxlaConstant expr) {
        toValue(expr);
    }

    private void visit(OxlaFunction<?> expr) {
        toValue(expr);
        for (OxlaExpression arg : expr.getArgs()) {
            visit(arg);
        }
    }

    private void visit(OxlaInOperator expr) {
        toValue(expr);
        visit(expr.getLeft());
        for (OxlaExpression right : expr.getRight()) {
            visit(right);
        }
    }

    private void visit(OxlaJoin expr) {
        toValue(expr.onClause);
    }

    private void visit(OxlaOrderingTerm expr) {
        toValue(expr);
        visit(expr.getExpr());
    }

    private void visit(OxlaPostfixText expr) {
        toValue(expr);
        visit(expr.getExpr());
    }

    private void visit(OxlaSelect expr) {
        toValue(expr.getWhereClause());
    }

    private void visit(OxlaTableReference expr) {
        toValue(expr);
    }

    private void visit(OxlaTernaryNode expr) {
        toValue(expr);
        visit(expr.getLeft());
        visit(expr.getMiddle());
        visit(expr.getRight());
    }

    private void visit(OxlaUnaryPostfixOperation expr) {
        toValue(expr);
        visit(expr.getExpr());
    }

    private void visit(OxlaUnaryPrefixOperation expr) {
        toValue(expr);
        visit(expr.getExpr());
    }

    private void toValue(OxlaExpression expr) {
        sb.append(OxlaToStringVisitor.asString(expr));
        sb.append(" -- ");
        sb.append(expr.getExpectedValue());
        sb.append('\n');
    }

    public String get() {
        return sb.toString();
    }
}
