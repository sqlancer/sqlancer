package sqlancer.doris.visitor;

import java.util.List;

import sqlancer.doris.ast.DorisAlias;
import sqlancer.doris.ast.DorisBetweenOperation;
import sqlancer.doris.ast.DorisBinaryOperation;
import sqlancer.doris.ast.DorisColumnReference;
import sqlancer.doris.ast.DorisConstant;
import sqlancer.doris.ast.DorisExpression;
import sqlancer.doris.ast.DorisFunction;
import sqlancer.doris.ast.DorisInOperation;
import sqlancer.doris.ast.DorisJoin;
import sqlancer.doris.ast.DorisOrderByTerm;
import sqlancer.doris.ast.DorisPostfixText;
import sqlancer.doris.ast.DorisSelect;
import sqlancer.doris.ast.DorisTableReference;
import sqlancer.doris.ast.DorisUnaryPostfixOperation;
import sqlancer.doris.ast.DorisUnaryPrefixOperation;

public class DorisExpectedValueVisitor {

    protected final StringBuilder sb = new StringBuilder();

    private void print(DorisExpression expr) {
        sb.append(DorisToStringVisitor.asString(expr));
        sb.append(" -- ");
        sb.append(((DorisExpression) expr).getExpectedValue());
        sb.append("\n");
    }

    public void visit(DorisExpression expr) {
        assert expr != null;
        if (expr instanceof DorisColumnReference) {
            visit((DorisColumnReference) expr);
        } else if (expr instanceof DorisUnaryPostfixOperation) {
            visit((DorisUnaryPostfixOperation) expr);
        } else if (expr instanceof DorisUnaryPrefixOperation) {
            visit((DorisUnaryPrefixOperation) expr);
        } else if (expr instanceof DorisBinaryOperation) {
            visit((DorisBinaryOperation) expr);
        } else if (expr instanceof DorisTableReference) {
            visit((DorisTableReference) expr);
        } else if (expr instanceof DorisFunction<?>) {
            visit((DorisFunction<?>) expr);
        } else if (expr instanceof DorisBetweenOperation) {
            visit((DorisBetweenOperation) expr);
        } else if (expr instanceof DorisInOperation) {
            visit((DorisInOperation) expr);
        } else if (expr instanceof DorisOrderByTerm) {
            visit((DorisOrderByTerm) expr);
        } else if (expr instanceof DorisAlias) {
            visit((DorisAlias) expr);
        } else if (expr instanceof DorisPostfixText) {
            visit((DorisPostfixText) expr);
        } else if (expr instanceof DorisConstant) {
            visit((DorisConstant) expr);
        } else if (expr instanceof DorisSelect) {
            visit((DorisSelect) expr);
        } else if (expr instanceof DorisJoin) {
            visit((DorisJoin) expr);
        } else {
            throw new AssertionError(expr);
        }
    }

    public void visit(DorisColumnReference c) {
        print(c);
    }

    public void visit(DorisUnaryPostfixOperation op) {
        print(op);
        visit(op.getExpr());
    }

    public void visit(DorisUnaryPrefixOperation op) {
        print(op);
        visit(op.getExpr());
    }

    public void visit(DorisBinaryOperation op) {
        visit(op.getLeft());
        visit(op.getRight());
    }

    public void visit(DorisTableReference t) {
        print(t);
    }

    public void visit(DorisFunction<?> fun) {
        print(fun);
        visit(fun.getArgs());
    }

    public void visit(List<DorisExpression> expressions) {
        for (DorisExpression expression : expressions) {
            visit(expression);
        }
    }

    public void visit(DorisBetweenOperation op) {
        print(op);
        visit(op.getLeft());
        visit(op.getMiddle());
        visit(op.getRight());
    }

    public void visit(DorisInOperation op) {
        print(op);
        visit(op.getLeft());
        visit(op.getRight());
    }

    public void visit(DorisOrderByTerm op) {
        print(op);
        visit(op.getExpr());
    }

    public void visit(DorisAlias op) {
        print(op);
        visit(op.getExpr());
    }

    public void visit(DorisPostfixText postFixText) {
        print(postFixText);
        visit(postFixText.getExpr());
    }

    public void visit(DorisConstant constant) {
        print(constant);
    }

    public void visit(DorisSelect select) {
        print(select.getWhereClause());
    }

    public void visit(DorisJoin join) {
        print(join.getOnCondition());
    }

    public String get() {
        return sb.toString();
    }

    public static String asExpectedValues(DorisExpression expr) {
        DorisExpectedValueVisitor v = new DorisExpectedValueVisitor();
        v.visit(expr);
        return v.get();
    }

}
