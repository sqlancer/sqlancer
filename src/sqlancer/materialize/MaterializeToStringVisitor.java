package sqlancer.materialize;

import java.util.Optional;

import sqlancer.Randomly;
import sqlancer.common.visitor.BinaryOperation;
import sqlancer.common.visitor.ToStringVisitor;
import sqlancer.materialize.MaterializeSchema.MaterializeDataType;
import sqlancer.materialize.ast.MaterializeAggregate;
import sqlancer.materialize.ast.MaterializeBetweenOperation;
import sqlancer.materialize.ast.MaterializeBinaryLogicalOperation;
import sqlancer.materialize.ast.MaterializeCastOperation;
import sqlancer.materialize.ast.MaterializeColumnValue;
import sqlancer.materialize.ast.MaterializeConstant;
import sqlancer.materialize.ast.MaterializeExpression;
import sqlancer.materialize.ast.MaterializeFunction;
import sqlancer.materialize.ast.MaterializeInOperation;
import sqlancer.materialize.ast.MaterializeJoin;
import sqlancer.materialize.ast.MaterializeJoin.MaterializeJoinType;
import sqlancer.materialize.ast.MaterializeLikeOperation;
import sqlancer.materialize.ast.MaterializeOrderByTerm;
import sqlancer.materialize.ast.MaterializePOSIXRegularExpression;
import sqlancer.materialize.ast.MaterializePostfixOperation;
import sqlancer.materialize.ast.MaterializePostfixText;
import sqlancer.materialize.ast.MaterializePrefixOperation;
import sqlancer.materialize.ast.MaterializeSelect;
import sqlancer.materialize.ast.MaterializeSelect.MaterializeFromTable;
import sqlancer.materialize.ast.MaterializeSelect.MaterializeSubquery;
import sqlancer.materialize.ast.MaterializeSimilarTo;

public final class MaterializeToStringVisitor extends ToStringVisitor<MaterializeExpression>
        implements MaterializeVisitor {

    @Override
    public void visitSpecific(MaterializeExpression expr) {
        MaterializeVisitor.super.visit(expr);
    }

    @Override
    public void visit(MaterializeConstant constant) {
        sb.append(constant.getTextRepresentation());
    }

    @Override
    public String get() {
        return sb.toString();
    }

    @Override
    public void visit(MaterializePostfixOperation op) {
        sb.append("(");
        visit(op.getExpression());
        sb.append(")");
        sb.append(" ");
        sb.append(op.getOperatorTextRepresentation());
    }

    @Override
    public void visit(MaterializeColumnValue c) {
        sb.append(c.getColumn().getFullQualifiedName());
    }

    @Override
    public void visit(MaterializePrefixOperation op) {
        sb.append(op.getTextRepresentation());
        sb.append(" (");
        visit(op.getExpression());
        sb.append(")");
    }

    @Override
    public void visit(MaterializeFromTable from) {
        sb.append(from.getTable().getName());
    }

    @Override
    public void visit(MaterializeSubquery subquery) {
        sb.append("(");
        visit(subquery.getSelect());
        sb.append(") AS ");
        sb.append(subquery.getName());
    }

    @Override
    public void visit(MaterializeSelect s) {
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
        visit(s.getFetchColumns());
        sb.append(" FROM ");
        visit(s.getFromList());

        for (MaterializeJoin j : s.getJoinClauses()) {
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
            case CROSS:
                sb.append("CROSS JOIN");
                break;
            default:
                throw new AssertionError(j.getType());
            }
            sb.append(" ");
            visit(j.getTableReference());
            if (j.getType() != MaterializeJoinType.CROSS) {
                sb.append(" ON ");
                visit(j.getOnClause());
            }
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
    public void visit(MaterializeOrderByTerm op) {
        visit(op.getExpr());
        sb.append(" ");
        sb.append(op.getOrder());
    }

    @Override
    public void visit(MaterializeFunction f) {
        sb.append(f.getFunctionName());
        sb.append("(");
        int i = 0;
        for (MaterializeExpression arg : f.getArguments()) {
            if (i++ != 0) {
                sb.append(", ");
            }
            visit(arg);
        }
        sb.append(")");
    }

    @Override
    public void visit(MaterializeCastOperation cast) {
        if (cast.getCompoundType().getDataType() == MaterializeDataType.BOOLEAN) {
            sb.append("(");
            MaterializeExpression expr = cast.getExpression();
            visit(expr);
            if (expr.getExpressionType() == MaterializeDataType.TEXT) {
                sb.append(" != '')");
            } else if (expr.getExpressionType() == MaterializeDataType.BOOLEAN) {
                sb.append(" != FALSE)");
            } else {
                sb.append(" != 0)");
            }
        } else if (Randomly.getBoolean()) {
            if (cast.getCompoundType().getDataType() == MaterializeDataType.REAL
                    || cast.getCompoundType().getDataType() == MaterializeDataType.FLOAT) {
                sb.append("CAST(CAST(");
                visit(cast.getExpression());
                sb.append(" AS INT) AS ");
                appendType(cast);
                sb.append(")");
            } else {
                sb.append("CAST(");
                visit(cast.getExpression());
                sb.append(" AS ");
                appendType(cast);
                sb.append(")");
            }
        } else {
            if (cast.getCompoundType().getDataType() == MaterializeDataType.REAL
                    || cast.getCompoundType().getDataType() == MaterializeDataType.FLOAT) {
                sb.append("(");
                visit(cast.getExpression());
                sb.append(")::INT::");
                appendType(cast);
            } else {
                sb.append("(");
                visit(cast.getExpression());
                sb.append(")::");
                appendType(cast);
            }
        }
    }

    private void appendType(MaterializeCastOperation cast) {
        MaterializeCompoundDataType compoundType = cast.getCompoundType();
        switch (compoundType.getDataType()) {
        case BOOLEAN:
            sb.append("BOOLEAN");
            break;
        case INT: // TODO support also other int types
            sb.append("INT");
            break;
        case TEXT:
            // TODO: append TEXT, CHAR
            sb.append(Randomly.fromOptions("VARCHAR"));
            break;
        case REAL:
            sb.append("FLOAT");
            break;
        case DECIMAL:
            sb.append("DECIMAL");
            break;
        case FLOAT:
            sb.append("REAL");
            break;
        case BIT:
            sb.append("INT");
            break;
        default:
            throw new AssertionError(cast.getType());
        }
        Optional<Integer> size = compoundType.getSize();
        if (size.isPresent()) {
            sb.append("(");
            sb.append(size.get());
            sb.append(")");
        }
    }

    @Override
    public void visit(MaterializeBetweenOperation op) {
        sb.append("(");
        visit(op.getExpr());
        sb.append(") BETWEEN ");
        sb.append("(");
        visit(op.getLeft());
        sb.append(") AND (");
        visit(op.getRight());
        sb.append(")");
    }

    @Override
    public void visit(MaterializeInOperation op) {
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
    public void visit(MaterializePostfixText op) {
        visit(op.getExpr());
        sb.append(op.getText());
    }

    @Override
    public void visit(MaterializeAggregate op) {
        sb.append(op.getFunction());
        sb.append("(");
        visit(op.getArgs());
        sb.append(")");
    }

    @Override
    public void visit(MaterializeSimilarTo op) {
        sb.append("(");
        visit(op.getString());
        sb.append(" SIMILAR TO ");
        visit(op.getSimilarTo());
        if (op.getEscapeCharacter() != null) {
            visit(op.getEscapeCharacter());
        }
        sb.append(")");
    }

    @Override
    public void visit(MaterializePOSIXRegularExpression op) {
        visit(op.getString());
        sb.append(op.getOp().getStringRepresentation());
        visit(op.getRegex());
    }

    @Override
    public void visit(MaterializeBinaryLogicalOperation op) {
        super.visit((BinaryOperation<MaterializeExpression>) op);
    }

    @Override
    public void visit(MaterializeLikeOperation op) {
        super.visit((BinaryOperation<MaterializeExpression>) op);
    }

}
