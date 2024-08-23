package sqlancer.databend;

import java.util.List;

import sqlancer.databend.ast.DatabendAlias;
import sqlancer.databend.ast.DatabendBetweenOperation;
import sqlancer.databend.ast.DatabendBinaryOperation;
import sqlancer.databend.ast.DatabendColumnReference;
import sqlancer.databend.ast.DatabendConstant;
import sqlancer.databend.ast.DatabendExpression;
import sqlancer.databend.ast.DatabendFunctionOperation;
import sqlancer.databend.ast.DatabendInOperation;
import sqlancer.databend.ast.DatabendJoin;
import sqlancer.databend.ast.DatabendOrderByTerm;
import sqlancer.databend.ast.DatabendPostFixText;
import sqlancer.databend.ast.DatabendSelect;
import sqlancer.databend.ast.DatabendTableReference;
import sqlancer.databend.ast.DatabendUnaryPostfixOperation;
import sqlancer.databend.ast.DatabendUnaryPrefixOperation;

public class DatabendExpectedValueVisitor {

    protected final StringBuilder sb = new StringBuilder();

    private void print(DatabendExpression expr) {
        sb.append(DatabendToStringVisitor.asString(expr));
        sb.append(" -- ");
        sb.append((expr).getExpectedValue());
        sb.append("\n");
    }

    public void visit(DatabendExpression expr) {
        assert expr != null;
        if (expr instanceof DatabendColumnReference) {
            visit((DatabendColumnReference) expr);
        } else if (expr instanceof DatabendUnaryPostfixOperation) {
            visit((DatabendUnaryPostfixOperation) expr);
        } else if (expr instanceof DatabendUnaryPrefixOperation) {
            visit((DatabendUnaryPrefixOperation) expr);
        } else if (expr instanceof DatabendBinaryOperation) {
            visit((DatabendBinaryOperation) expr);
        } else if (expr instanceof DatabendTableReference) {
            visit((DatabendTableReference) expr);
        } else if (expr instanceof DatabendFunctionOperation<?>) {
            visit((DatabendFunctionOperation<?>) expr);
        } else if (expr instanceof DatabendBetweenOperation) {
            visit((DatabendBetweenOperation) expr);
        } else if (expr instanceof DatabendInOperation) {
            visit((DatabendInOperation) expr);
        } else if (expr instanceof DatabendOrderByTerm) {
            visit((DatabendOrderByTerm) expr);
        } else if (expr instanceof DatabendAlias) {
            visit((DatabendAlias) expr);
        } else if (expr instanceof DatabendPostFixText) {
            visit((DatabendPostFixText) expr);
        } else if (expr instanceof DatabendConstant) {
            visit((DatabendConstant) expr);
        } else if (expr instanceof DatabendSelect) {
            visit((DatabendSelect) expr);
        } else if (expr instanceof DatabendJoin) {
            visit((DatabendJoin) expr);
        } else {
            throw new AssertionError(expr);
        }
    }

    public void visit(DatabendColumnReference c) {
        print(c);
    }

    public void visit(DatabendUnaryPostfixOperation op) {
        print(op);
        visit(op.getExpr());
    }

    public void visit(DatabendUnaryPrefixOperation op) {
        print(op);
        visit(op.getExpr());
    }

    public void visit(DatabendBinaryOperation op) {
        print(op);
        visit(op.getLeft());
        visit(op.getRight());
    }

    public void visit(DatabendTableReference t) {
        print(t);
    }

    public void visit(DatabendFunctionOperation<?> fun) {
        print(fun);
        visit(fun.getArgs());
    }

    public void visit(List<DatabendExpression> expressions) {
        for (DatabendExpression expression : expressions) {
            visit(expression);
        }
    }

    public void visit(DatabendBetweenOperation op) {
        print(op);
        visit(op.getLeft());
        visit(op.getMiddle());
        visit(op.getRight());
    }

    public void visit(DatabendInOperation op) {
        print(op);
        visit(op.getLeft());
        visit(op.getRight());
    }

    public void visit(DatabendOrderByTerm op) {
        print(op);
        visit(op.getExpr());
    }

    public void visit(DatabendAlias op) {
        print(op);
        visit(op.getExpr());
    }

    public void visit(DatabendPostFixText postFixText) {
        print(postFixText);
        visit(postFixText.getExpr());
    }

    public void visit(DatabendConstant constant) {
        print(constant);
    }

    public void visit(DatabendSelect select) {
        print(select.getWhereClause());
    }

    public void visit(DatabendJoin join) {
        print(join.getOnCondition());
    }

    public String get() {
        return sb.toString();
    }

    public static String asExpectedValues(DatabendExpression expr) {
        DatabendExpectedValueVisitor v = new DatabendExpectedValueVisitor();
        v.visit(expr);
        return v.get();
    }

}
