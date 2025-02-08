package sqlancer.common.visitor;

import sqlancer.Randomly;
import sqlancer.common.ast.SelectBase;
import sqlancer.common.ast.newast.Expression;
import sqlancer.common.ast.newast.Join;

public abstract class SelectToStringVisitor<
        T extends Expression<?>,
        S extends SelectBase<T>,
        J extends Join<T, ?, ?>
        > extends ToStringVisitor<T> {

    protected void visitSelect(S select) {
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

    protected void visitSelectOption(S select) {
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

    protected void visitDistinctOnClause(S select) {
        if (hasDistinctOnSupport() && getDistinctOnClause(select) != null) {
            sb.append("ON (");
            visit(getDistinctOnClause(select));
            sb.append(") ");
        }
    }

    protected abstract T getDistinctOnClause(S select);

    protected void visitColumns(S select) {
        if (select.getFetchColumns() == null) {
            sb.append("*");
        } else {
            visit(select.getFetchColumns());
        }
        sb.append(" FROM ");
    }

    protected void visitFromClause(S select) {
        visit(select.getFromList());
    }

    protected abstract void visitJoinClauses(S select);

    protected void visitJoinClause(J join) {
        sb.append(" ");
        visitJoinType(join);
        sb.append(" ");
        visit(getJoinTableReference(join));
        if (shouldVisitOnClause(join)) {
            sb.append(" ON ");
            visit(getJoinOnClause(join));
        }
    }

    protected abstract T getJoinOnClause(J join);

    protected abstract T getJoinTableReference(J join);

    protected abstract void visitJoinType(J join);

    protected abstract boolean shouldVisitOnClause(J join);

    protected void visitWhereClause(S select) {
        if (select.getWhereClause() != null) {
            sb.append(" WHERE ");
            visit(select.getWhereClause());
        }
    }

    protected void visitGroupByClause(S select) {
        if (select.getGroupByExpressions().size() > 0) {
            sb.append(" GROUP BY ");
            visit(select.getGroupByExpressions());
        }
    }

    protected void visitHavingClause(S select) {
        if (select.getHavingClause() != null) {
            sb.append(" HAVING ");
            visit(select.getHavingClause());
        }
    }

    protected void visitOrderByClause(S select) {
        if (!select.getOrderByClauses().isEmpty()) {
            sb.append(" ORDER BY ");
            visit(select.getOrderByClauses());
        }
    }

    protected void visitLimitClause(S select) {
        if (select.getLimitClause() != null) {
            sb.append(" LIMIT ");
            visit(select.getLimitClause());
        }
    }

    protected void visitOffsetClause(S select) {
        if (select.getOffsetClause() != null) {
            sb.append(" OFFSET ");
            visit(select.getOffsetClause());
        }
    }

    protected boolean hasDistinctOnSupport() {
        return false;
    }
}
