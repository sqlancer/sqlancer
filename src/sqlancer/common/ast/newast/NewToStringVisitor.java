package sqlancer.common.ast.newast;

import java.util.List;

public abstract class NewToStringVisitor<E> {

    protected final StringBuilder sb = new StringBuilder();

    @SuppressWarnings("unchecked")
    public void visit(Node<E> expr) {
        assert expr != null;
        if (expr instanceof ColumnReferenceNode<?, ?>) {
            sb.append(((ColumnReferenceNode<?, ?>) expr).getColumn().getFullQualifiedName());
        } else if (expr instanceof NewUnaryPostfixOperatorNode<?>) {
            visit((NewUnaryPostfixOperatorNode<E>) expr);
        } else if (expr instanceof NewUnaryPrefixOperatorNode<?>) {
            visit((NewUnaryPrefixOperatorNode<E>) expr);
        } else if (expr instanceof NewBinaryOperatorNode<?>) {
            visit((NewBinaryOperatorNode<E>) expr);
        } else if (expr instanceof TableReferenceNode<?, ?>) {
            visit((TableReferenceNode<E, ?>) expr);
        } else if (expr instanceof NewFunctionNode<?, ?>) {
            visit((NewFunctionNode<E, ?>) expr);
        } else if (expr instanceof NewBetweenOperatorNode<?>) {
            visit((NewBetweenOperatorNode<E>) expr);
        } else if (expr instanceof NewInOperatorNode<?>) {
            visit((NewInOperatorNode<E>) expr);
        } else if (expr instanceof NewCaseOperatorNode<?>) {
            visit((NewCaseOperatorNode<E>) expr);
        } else if (expr instanceof NewOrderingTerm<?>) {
            visit((NewOrderingTerm<E>) expr);
        } else if (expr instanceof NewAliasNode<?>) {
            visit((NewAliasNode<E>) expr);
        } else if (expr instanceof NewPostfixTextNode<?>) {
            visit((NewPostfixTextNode<E>) expr);
        } else if (expr instanceof NewTernaryNode<?>) {
            visit((NewTernaryNode<E>) expr);
        } else {
            visitSpecific(expr);
        }
    }

    public void visit(List<Node<E>> expressions) {
        for (int i = 0; i < expressions.size(); i++) {
            if (i != 0) {
                sb.append(", ");
            }
            visit(expressions.get(i));
        }
    }

    public void visit(NewPostfixTextNode<E> postFixText) {
        visit(postFixText.getExpr());
        sb.append(postFixText.getText());
    }

    public void visit(TableReferenceNode<E, ?> tableRef) {
        sb.append(tableRef.getTable().getName());
    }

    public void visit(NewAliasNode<E> alias) {
        visit(alias.getExpr());
        sb.append(" AS ");
        sb.append(alias.getAlias());
    }

    public void visit(NewOrderingTerm<E> ordering) {
        visit(ordering.getExpr());
        sb.append(" ");
        sb.append(ordering.getOrdering());
    }

    public void visit(NewCaseOperatorNode<E> op) {
        sb.append("(CASE ");
        visit(op.getSwitchCondition());
        for (int i = 0; i < op.getConditions().size(); i++) {
            sb.append(" WHEN ");
            visit(op.getConditions().get(i));
            sb.append(" THEN ");
            visit(op.getExpressions().get(i));
        }
        if (op.getElseExpr() != null) {
            sb.append(" ELSE ");
            visit(op.getElseExpr());
        }
        sb.append(" END )");
    }

    public void visit(NewBetweenOperatorNode<E> opNode) {
        sb.append("(");
        visit(opNode.getLeft());
        if (!opNode.isTrue()) {
            sb.append(" NOT");
        }
        sb.append(" BETWEEN ");
        visit(opNode.getMiddle());
        sb.append(" AND ");
        visit(opNode.getRight());
        sb.append(")");
    }

    public void visit(NewUnaryPostfixOperatorNode<E> opNode) {
        sb.append("((");
        visit(opNode.getExpr());
        sb.append(") ");
        sb.append(opNode.getOperatorRepresentation());
        sb.append(")");
    }

    public void visit(NewFunctionNode<E, ?> funcCall) {
        sb.append(funcCall.getFunc().toString());
        sb.append("(");
        visit(funcCall.getArgs());
        sb.append(")");
    }

    public void visit(NewInOperatorNode<E> in) {
        sb.append("(");
        visit(in.getLeft());
        if (in.isNegated()) {
            sb.append(" NOT");
        }
        sb.append(" IN (");
        visit(in.getRight());
        sb.append("))");
    }

    public void visit(NewUnaryPrefixOperatorNode<E> opNode) {
        sb.append("(");
        sb.append(opNode.getOperatorRepresentation());
        sb.append(" ");
        visit(opNode.getExpr());
        sb.append(")");
    }

    public void visit(NewBinaryOperatorNode<E> opNode) {
        sb.append("(");
        sb.append("(");

        visit(opNode.getLeft());
        sb.append(")");
        sb.append(opNode.getOperatorRepresentation());
        sb.append("(");

        visit(opNode.getRight());
        sb.append(")");
        sb.append(")");
    }

    public void visit(NewTernaryNode<E> ternaryNode) {
        sb.append("(");
        visit(ternaryNode.getLeft());
        sb.append(" ");
        sb.append(ternaryNode.getLeftStr());
        sb.append(" ");
        visit(ternaryNode.getMiddle());
        sb.append(" ");
        sb.append(ternaryNode.getRightStr());
        sb.append(" ");
        visit(ternaryNode.getRight());
        sb.append(")");
    }

    public String get() {
        return sb.toString();
    }

    public abstract void visitSpecific(Node<E> expr);

}
