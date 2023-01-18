package sqlancer.cnosdb;

import sqlancer.Randomly;
import sqlancer.cnosdb.ast.*;
import sqlancer.cnosdb.ast.CnosDBSelect.CnosDBFromTable;
import sqlancer.cnosdb.ast.CnosDBSelect.CnosDBSubquery;
import sqlancer.common.visitor.BinaryOperation;
import sqlancer.common.visitor.ToStringVisitor;

public final class CnosDBToStringVisitor extends ToStringVisitor<CnosDBExpression> implements CnosDBVisitor {

    @Override
    public void visitSpecific(CnosDBExpression expr) {
        CnosDBVisitor.super.visit(expr);
    }

    @Override
    public void visit(CnosDBConstant constant) {
        sb.append(constant.getTextRepresentation());
    }

    @Override
    public String get() {
        return sb.toString();
    }

    @Override
    public void visit(CnosDBPostfixOperation op) {
        sb.append("(");
        visit(op.getExpression());
        sb.append(")");
        sb.append(" ");
        sb.append(op.getOperatorTextRepresentation());
    }

    @Override
    public void visit(CnosDBColumnValue c) {
        sb.append(c.getColumn().getFullQualifiedName());
    }

    @Override
    public void visit(CnosDBPrefixOperation op) {
        sb.append(op.getTextRepresentation());
        sb.append(" (");
        visit(op.getExpression());
        sb.append(")");
    }

    @Override
    public void visit(CnosDBFromTable from) {
        sb.append(from.getTable().getName());
    }

    @Override
    public void visit(CnosDBSubquery subquery) {
        sb.append("(");
        visit(subquery.getSelect());
        sb.append(") AS ");
        sb.append(subquery.getName());
    }

    @Override
    public void visit(CnosDBSelect s) {
        sb.append("SELECT ");
        switch (s.getSelectOption()) {
        case DISTINCT:
            sb.append("DISTINCT ");
            if (s.getDistinctOnClause() != null) {
                sb.append("ON (");
                visit(s.getDistinctOnClause());
                sb.append(") ");
            }
            break;
        case ALL:
            sb.append(Randomly.fromOptions("ALL ", ""));
            break;
        default:
            throw new AssertionError();
        }
        if (s.getFetchColumns() == null) {
            sb.append("*");
        } else {
            visit(s.getFetchColumns());
        }
        sb.append(" FROM ");
        visit(s.getFromList());

        for (CnosDBJoin j : s.getJoinClauses()) {
            sb.append(" ");
            switch (j.getType()) {
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
            // case CROSS:
            // sb.append("CROSS JOIN");
            // break;
            default:
                throw new AssertionError(j.getType());
            }
            sb.append(" ");
            visit(j.getTableReference());
            // if (j.getType() != CnosDBJoinType.CROSS) {
            sb.append(" ON ");
            visit(j.getOnClause());
            // }
        }

        if (s.getWhereClause() != null) {
            sb.append(" WHERE ");
            visit(s.getWhereClause());
        }
        if (s.getGroupByExpressions().size() > 0) {
            sb.append(" GROUP BY ");
            visit(s.getGroupByExpressions());
        }
        if (s.getHavingClause() != null) {
            sb.append(" HAVING ");
            visit(s.getHavingClause());

        }
        if (!s.getOrderByExpressions().isEmpty()) {
            sb.append(" ORDER BY ");
            visit(s.getOrderByExpressions());
        }
        if (s.getLimitClause() != null) {
            sb.append(" LIMIT ");
            visit(s.getLimitClause());
        }

        if (s.getOffsetClause() != null) {
            sb.append(" OFFSET ");
            visit(s.getOffsetClause());
        }
    }

    @Override
    public void visit(CnosDBOrderByTerm op) {
        visit(op.getExpr());
        sb.append(" ");
        sb.append(op.getOrder());
    }

    @Override
    public void visit(CnosDBFunction f) {
        sb.append(f.getFunctionName());
        sb.append("(");
        int i = 0;
        for (CnosDBExpression arg : f.getArguments()) {
            if (i++ != 0) {
                sb.append(", ");
            }
            visit(arg);
        }
        sb.append(")");
    }

    @Override
    public void visit(CnosDBCastOperation cast) {
        sb.append("CAST( ");
        visit(cast.getExpression());
        sb.append(" AS ");
        appendType(cast);
        sb.append(")");
    }

    private void appendType(CnosDBCastOperation cast) {
        CnosDBCompoundDataType compoundType = cast.getCompoundType();
        switch (compoundType.getDataType()) {
        case BOOLEAN:
            sb.append("BOOLEAN");
            break;
        case INT:
            sb.append("BIGINT");
            break;
        case STRING:
            sb.append(Randomly.fromOptions("STRING"));
            break;
        case DOUBLE:
            sb.append("DOUBLE");
            break;
        case UINT:
            sb.append("BIGINT UNSIGNED");
            break;
        case TIMESTAMP:
            sb.append("TIMESTAMP");
            break;

        default:
            throw new AssertionError(cast.getType());
        }
    }

    @Override
    public void visit(CnosDBBetweenOperation op) {
        sb.append("(");
        visit(op.getExpr());
        sb.append(") BETWEEN (");
        visit(op.getLeft());
        sb.append(") AND (");
        visit(op.getRight());
        sb.append(")");
    }

    @Override
    public void visit(CnosDBInOperation op) {
        sb.append("(");
        visit(op.getExpr());
        sb.append(")");
        if (!op.isTrue()) {
            sb.append(" NOT");
        }
        sb.append(" IN (");
        visit(op.getListElements());
        sb.append(")");
    }

    @Override
    public void visit(CnosDBPostfixText op) {
        visit(op.getExpr());
        sb.append(op.getText());
    }

    @Override
    public void visit(CnosDBAggregate op) {
        sb.append(op.getFunction());
        sb.append("(");
        visit(op.getArgs());
        sb.append(")");
    }

    @Override
    public void visit(CnosDBSimilarTo op) {
        sb.append("(");
        visit(op.getString());
        sb.append(" SIMILAR TO ");
        visit(op.getSimilarTo());
        sb.append(")");
    }

    @Override
    public void visit(CnosDBBinaryLogicalOperation op) {
        super.visit((BinaryOperation<CnosDBExpression>) op);
    }

    @Override
    public void visit(CnosDBLikeOperation op) {
        super.visit((BinaryOperation<CnosDBExpression>) op);
    }

}
