package sqlancer.common.visitor;

import java.util.List;

import sqlancer.Randomly;
import sqlancer.common.ast.JoinBase;
import sqlancer.common.ast.SelectBase;
import sqlancer.common.ast.newast.Expression;
import sqlancer.common.visitor.UnaryOperation.OperatorKind;

public abstract class ToStringVisitor<T extends Expression<?>> extends NodeVisitor<T> {

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

    protected void visitSelect(SelectBase<T> select) {
        sb.append("SELECT ");
        visitSelectOption(select);
        visitColumns(select);
        visitFromClause(select);
        visitJoinClauses(select);
        visitWhereClause(select);
        visitGroupByClause(select);
        visitHavingClause(select);
        visitOrderByClause(select);
        visitLimitClause(select);
        visitOffsetClause(select);
    }

    protected void visitSelectOption(SelectBase<T> select) {
        switch (select.getSelectOption()) {
        case DISTINCT:
            sb.append("DISTINCT ");
            visitDistinctOnClause(select);
            break;
        case ALL:
            sb.append(Randomly.fromOptions("ALL ", ""));
            break;
        default:
            throw new AssertionError();
        }
    }

    protected void visitDistinctOnClause(SelectBase<T> select) {
        //todo
        if (hasDistinctOnSupport() && getDistinctOnClause(select) != null) {
            sb.append("ON (");
            visit(getDistinctOnClause(select));
            sb.append(") ");
        }
    }

    protected abstract T getDistinctOnClause(SelectBase<T> select);


    protected void visitColumns(SelectBase<T> select) {
        if (select.getFetchColumns() == null) {
            sb.append("*");
        } else {
            visit(select.getFetchColumns());
        }
        sb.append(" FROM ");
    }

    protected void visitFromClause(SelectBase<T> select) {
        visit(select.getFromList());
    }

    protected void visitJoinClauses(SelectBase<T> select){
        for (JoinBase join : select.getJoinClauses()) {
            visitJoinClause(join);
        }
    }


    protected void visitJoinClause(JoinBase<T> join) {
        sb.append(" ");
        visitJoinType(join);
        sb.append(" ");
        visit(getJoinTableReference(join));
        if (shouldVisitOnClause(join)) {
            sb.append(" ON ");
            visit(getJoinOnClause(join));
        }
    }

    protected abstract T getJoinOnClause(JoinBase<T> join);

    protected abstract T getJoinTableReference(JoinBase<T> join);

    protected void visitJoinType(JoinBase<T> join) {
        switch (join.getType()) {
        case INNER:
            if (Randomly.getBoolean()) {
                sb.append("INNER ");
            }
            sb.append("JOIN");
            break;
        case LEFT:
            sb.append("LEFT OUTER JOIN");
            break;
        case RIGHT:
            sb.append("RIGHT OUTER JOIN");
            break;
        case FULL:
            sb.append("FULL OUTER JOIN");
            break;
        case CROSS:
            sb.append("CROSS JOIN");
            break;
        default:
            throw new AssertionError(join.getType());
        }
    }

    protected boolean shouldVisitOnClause(JoinBase<T> join) {
        return join.getType() != JoinBase.JoinType.CROSS;
    }


    protected void visitWhereClause(SelectBase<T> select) {
        if (select.getWhereClause() != null) {
            sb.append(" WHERE ");
            visit(select.getWhereClause());
        }
    }

    protected void visitGroupByClause(SelectBase<T> select) {
        if (select.getGroupByExpressions().size() > 0) {
            sb.append(" GROUP BY ");
            visit(select.getGroupByExpressions());
        }
    }

    protected void visitHavingClause(SelectBase<T> select) {
        if (select.getHavingClause() != null) {
            sb.append(" HAVING ");
            visit(select.getHavingClause());
        }
    }

    protected void visitOrderByClause(SelectBase<T> select) {
        if (!select.getOrderByClauses().isEmpty()) {
            sb.append(" ORDER BY ");
            visit(select.getOrderByClauses());
        }
    }

    protected void visitLimitClause(SelectBase<T> select) {
        if (select.getLimitClause() != null) {
            sb.append(" LIMIT ");
            visit(select.getLimitClause());
        }
    }

    protected void visitOffsetClause(SelectBase<T> select) {
        if (select.getOffsetClause() != null) {
            sb.append(" OFFSET ");
            visit(select.getOffsetClause());
        }
    }

    protected boolean hasDistinctOnSupport() {
        return false;
    }

}
