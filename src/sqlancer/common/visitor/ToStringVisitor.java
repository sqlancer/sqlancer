package sqlancer.common.visitor;

import java.util.List;

import sqlancer.common.visitor.UnaryOperation.OperatorKind;

public abstract class ToStringVisitor<T> extends NodeVisitor<T> {

    protected final StringBuilder sb = new StringBuilder();

    public void visit(BinaryOperation<T> op) {
        sb.append('(');
        sb.append('(');
        visit(op.getLeft());
        sb.append(')');
        sb.append(op.getOperatorRepresentation());
        sb.append('(');
        visit(op.getRight());
        sb.append(')');
        sb.append(')');
    }

    public void visit(UnaryOperation<T> op) {
        if (!op.omitBracketsWhenPrinting()) {
            sb.append('(');
        }
        if (op.getOperatorKind() == OperatorKind.PREFIX) {
            sb.append(op.getOperatorRepresentation());
            sb.append(' ');
        }
        if (!op.omitBracketsWhenPrinting()) {
            sb.append('(');
        }
        visit(op.getExpression());
        if (!op.omitBracketsWhenPrinting()) {
            sb.append(')');
        }
        if (op.getOperatorKind() == OperatorKind.POSTFIX) {
            sb.append(' ');
            sb.append(op.getOperatorRepresentation());
        }
        if (!op.omitBracketsWhenPrinting()) {
            sb.append(')');
        }
    }

    @SuppressWarnings("unchecked")
    public void visit(T expr) {
        assert expr != null;
        if (expr instanceof BinaryOperation<?>) {
            visit((BinaryOperation<T>) expr);
        } else if (expr instanceof UnaryOperation<?>) {
            visit((UnaryOperation<T>) expr);
        } else {
            visitSpecific(expr);
        }
    }

    public abstract void visitSpecific(T expr);

    public void visit(List<T> expressions) {
        for (int i = 0; i < expressions.size(); i++) {
            if (i != 0) {
                sb.append(", ");
            }
            visit(expressions.get(i));
        }
    }

    public String get() {
        return sb.toString();
    }

}
