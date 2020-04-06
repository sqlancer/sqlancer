package sqlancer.ast.newast;

import java.util.List;

public abstract class NewToStringVisitor<E> {

	protected final StringBuilder sb = new StringBuilder();

	@SuppressWarnings("unchecked")
	public void visit(Node<E> expr) {
		assert expr != null;
		if (expr instanceof ColumnReferenceNode<?, ?>) {
			sb.append(((ColumnReferenceNode<?, ?>) expr).getColumn().getName());
		} else if (expr instanceof NewUnaryPostfixOperatorNode<?>) {
			visit((NewUnaryPostfixOperatorNode<E>) expr);
		} else if (expr instanceof NewUnaryPrefixOperatorNode<?>) {
			visit((NewUnaryPrefixOperatorNode<E>) expr);
		} else if (expr instanceof NewBinaryOperatorNode<?>) {
			visit((NewBinaryOperatorNode<E>) expr);
		} else if (expr instanceof TableReferenceNode<?, ?>) {
			visit((TableReferenceNode<E, ?>) expr);
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

	public void visit(TableReferenceNode<E, ?> tableRef) {
		sb.append(tableRef.getTable().getName());
	}

	public void visit(NewUnaryPostfixOperatorNode<E> opNode) {
		sb.append("(");
		visit(opNode.getExpr());
		sb.append(" ");
		sb.append(opNode.getOperatorRepresentation());
		sb.append(")");
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

	public String get() {
		return sb.toString();
	}

	public abstract void visitSpecific(Node<E> expr);

}
