package sqlancer.databend;

import java.util.List;

import sqlancer.common.ast.newast.ColumnReferenceNode;
import sqlancer.common.ast.newast.NewAliasNode;
import sqlancer.common.ast.newast.NewBetweenOperatorNode;
import sqlancer.common.ast.newast.NewBinaryOperatorNode;
import sqlancer.common.ast.newast.NewFunctionNode;
import sqlancer.common.ast.newast.NewInOperatorNode;
import sqlancer.common.ast.newast.NewOrderingTerm;
import sqlancer.common.ast.newast.NewPostfixTextNode;
import sqlancer.common.ast.newast.NewUnaryPostfixOperatorNode;
import sqlancer.common.ast.newast.NewUnaryPrefixOperatorNode;
import sqlancer.common.ast.newast.Node;
import sqlancer.common.ast.newast.TableReferenceNode;
import sqlancer.databend.DatabendSchema.DatabendColumn;
import sqlancer.databend.ast.DatabendConstant;
import sqlancer.databend.ast.DatabendExpression;
import sqlancer.databend.ast.DatabendJoin;
import sqlancer.databend.ast.DatabendSelect;

public class DatabendExpectedValueVisitor {

    protected final StringBuilder sb = new StringBuilder();

    private void print(Node<DatabendExpression> expr) {
        sb.append(DatabendToStringVisitor.asString(expr));
        sb.append(" -- ");
        sb.append(((DatabendExpression) expr).getExpectedValue());
        sb.append("\n");
    }

    @SuppressWarnings("unchecked")
    public void visit(Node<DatabendExpression> expr) {
        assert expr != null;
        if (expr instanceof ColumnReferenceNode<?, ?>) {
            visit((ColumnReferenceNode<DatabendExpression, DatabendColumn>) expr);
        } else if (expr instanceof NewUnaryPostfixOperatorNode<?>) {
            visit((NewUnaryPostfixOperatorNode<DatabendExpression>) expr);
        } else if (expr instanceof NewUnaryPrefixOperatorNode<?>) {
            visit((NewUnaryPrefixOperatorNode<DatabendExpression>) expr);
        } else if (expr instanceof NewBinaryOperatorNode<?>) {
            visit((NewBinaryOperatorNode<DatabendExpression>) expr);
        } else if (expr instanceof TableReferenceNode<?, ?>) {
            visit((TableReferenceNode<DatabendExpression, ?>) expr);
        } else if (expr instanceof NewFunctionNode<?, ?>) {
            visit((NewFunctionNode<DatabendExpression, ?>) expr);
        } else if (expr instanceof NewBetweenOperatorNode<?>) {
            visit((NewBetweenOperatorNode<DatabendExpression>) expr);
        } else if (expr instanceof NewInOperatorNode<?>) {
            visit((NewInOperatorNode<DatabendExpression>) expr);
        } else if (expr instanceof NewOrderingTerm<?>) {
            visit((NewOrderingTerm<DatabendExpression>) expr);
        } else if (expr instanceof NewAliasNode<?>) {
            visit((NewAliasNode<DatabendExpression>) expr);
        } else if (expr instanceof NewPostfixTextNode<?>) {
            visit((NewPostfixTextNode<DatabendExpression>) expr);
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

    public void visit(ColumnReferenceNode<DatabendExpression, DatabendColumn> c) {
        print(c);
    }

    public void visit(NewUnaryPostfixOperatorNode<DatabendExpression> op) {
        print(op);
        visit(op.getExpr());
    }

    public void visit(NewUnaryPrefixOperatorNode<DatabendExpression> op) {
        print(op);
        visit(op.getExpr());
    }

    public void visit(NewBinaryOperatorNode<DatabendExpression> op) {
        print(op);
        visit(op.getLeft());
        visit(op.getRight());
    }

    public void visit(TableReferenceNode<DatabendExpression, ?> t) {
        print(t);
    }

    public void visit(NewFunctionNode<DatabendExpression, ?> fun) {
        print(fun);
        visit(fun.getArgs());
    }

    public void visit(List<Node<DatabendExpression>> expressions) {
        for (Node<DatabendExpression> expression : expressions) {
            visit(expression);
        }
    }

    public void visit(NewBetweenOperatorNode<DatabendExpression> op) {
        print(op);
        visit(op.getLeft());
        visit(op.getMiddle());
        visit(op.getRight());
    }

    public void visit(NewInOperatorNode<DatabendExpression> op) {
        print(op);
        visit(op.getLeft());
        visit(op.getRight());
    }

    public void visit(NewOrderingTerm<DatabendExpression> op) {
        print(op);
        visit(op.getExpr());
    }

    public void visit(NewAliasNode<DatabendExpression> op) {
        print(op);
        visit(op.getExpr());
    }

    public void visit(NewPostfixTextNode<DatabendExpression> postFixText) {
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

    public static String asExpectedValues(Node<DatabendExpression> expr) {
        DatabendExpectedValueVisitor v = new DatabendExpectedValueVisitor();
        v.visit(expr);
        return v.get();
    }

}
