package sqlancer.doris.visitor;

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
import sqlancer.doris.DorisSchema.DorisColumn;
import sqlancer.doris.ast.DorisConstant;
import sqlancer.doris.ast.DorisExpression;
import sqlancer.doris.ast.DorisJoin;
import sqlancer.doris.ast.DorisSelect;

public class DorisExpectedValueVisitor {

    protected final StringBuilder sb = new StringBuilder();

    private void print(Node<DorisExpression> expr) {
        sb.append(DorisToStringVisitor.asString(expr));
        sb.append(" -- ");
        sb.append(((DorisExpression) expr).getExpectedValue());
        sb.append("\n");
    }

    @SuppressWarnings("unchecked")
    public void visit(Node<DorisExpression> expr) {
        assert expr != null;
        if (expr instanceof ColumnReferenceNode<?, ?>) {
            visit((ColumnReferenceNode<DorisExpression, DorisColumn>) expr);
        } else if (expr instanceof NewUnaryPostfixOperatorNode<?>) {
            visit((NewUnaryPostfixOperatorNode<DorisExpression>) expr);
        } else if (expr instanceof NewUnaryPrefixOperatorNode<?>) {
            visit((NewUnaryPrefixOperatorNode<DorisExpression>) expr);
        } else if (expr instanceof NewBinaryOperatorNode<?>) {
            visit((NewBinaryOperatorNode<DorisExpression>) expr);
        } else if (expr instanceof TableReferenceNode<?, ?>) {
            visit((TableReferenceNode<DorisExpression, ?>) expr);
        } else if (expr instanceof NewFunctionNode<?, ?>) {
            visit((NewFunctionNode<DorisExpression, ?>) expr);
        } else if (expr instanceof NewBetweenOperatorNode<?>) {
            visit((NewBetweenOperatorNode<DorisExpression>) expr);
        } else if (expr instanceof NewInOperatorNode<?>) {
            visit((NewInOperatorNode<DorisExpression>) expr);
        } else if (expr instanceof NewOrderingTerm<?>) {
            visit((NewOrderingTerm<DorisExpression>) expr);
        } else if (expr instanceof NewAliasNode<?>) {
            visit((NewAliasNode<DorisExpression>) expr);
        } else if (expr instanceof NewPostfixTextNode<?>) {
            visit((NewPostfixTextNode<DorisExpression>) expr);
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

    public void visit(ColumnReferenceNode<DorisExpression, DorisColumn> c) {
        print(c);
    }

    public void visit(NewUnaryPostfixOperatorNode<DorisExpression> op) {
        print(op);
        visit(op.getExpr());
    }

    public void visit(NewUnaryPrefixOperatorNode<DorisExpression> op) {
        print(op);
        visit(op.getExpr());
    }

    public void visit(NewBinaryOperatorNode<DorisExpression> op) {
        print(op);
        visit(op.getLeft());
        visit(op.getRight());
    }

    public void visit(TableReferenceNode<DorisExpression, ?> t) {
        print(t);
    }

    public void visit(NewFunctionNode<DorisExpression, ?> fun) {
        print(fun);
        visit(fun.getArgs());
    }

    public void visit(List<Node<DorisExpression>> expressions) {
        for (Node<DorisExpression> expression : expressions) {
            visit(expression);
        }
    }

    public void visit(NewBetweenOperatorNode<DorisExpression> op) {
        print(op);
        visit(op.getLeft());
        visit(op.getMiddle());
        visit(op.getRight());
    }

    public void visit(NewInOperatorNode<DorisExpression> op) {
        print(op);
        visit(op.getLeft());
        visit(op.getRight());
    }

    public void visit(NewOrderingTerm<DorisExpression> op) {
        print(op);
        visit(op.getExpr());
    }

    public void visit(NewAliasNode<DorisExpression> op) {
        print(op);
        visit(op.getExpr());
    }

    public void visit(NewPostfixTextNode<DorisExpression> postFixText) {
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

    public static String asExpectedValues(Node<DorisExpression> expr) {
        DorisExpectedValueVisitor v = new DorisExpectedValueVisitor();
        v.visit(expr);
        return v.get();
    }

}
