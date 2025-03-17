package sqlancer.common.visitor;

import java.util.List;
import java.util.Optional;

import sqlancer.Randomly;
import sqlancer.common.ast.JoinBase;
import sqlancer.common.ast.SelectBase;
import sqlancer.common.ast.newast.Expression;
import sqlancer.common.schema.AbstractBinaryLogicalOperation;
import sqlancer.common.schema.AbstractCastOperation;
import sqlancer.common.schema.AbstractCompoundDataType;
import sqlancer.common.schema.AbstractInOperation;
import sqlancer.common.schema.AbstractPostfixOperation;
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
        // todo
        if (hasDistinctOnSupport() && getDistinctOnClause(select) != null) {
            sb.append("ON (");
            visit(getDistinctOnClause(select));
            sb.append(") ");
        }
    }

    protected T getDistinctOnClause(SelectBase<T> select) {
        return select.getDistinctOnClause();
    }

    protected void visitColumns(SelectBase<T> select) {
        if (select.getFetchColumns() == null) {
            sb.append("*");
        } else {
            visit(select.getFetchColumns());
        }
    }

    protected void visitFromClause(SelectBase<T> select) {
        sb.append(" FROM ");
        visit(select.getFromList());
    }

    protected void visitJoinClauses(SelectBase<T> select) {
        for (JoinBase<T> join : select.getJoinClauses()) {
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

    protected void visitBasicJoinType(JoinBase<T> join) {
        sb.append(" ");
        switch (join.getType()) {
        case NATURAL:
            sb.append("NATURAL ");
            break;
        case INNER:
            sb.append("INNER ");
            break;
        case STRAIGHT:
            sb.append("STRAIGHT_");
            break;
        case LEFT:
            sb.append("LEFT ");
            break;
        case RIGHT:
            sb.append("RIGHT ");
            break;
        case CROSS:
            sb.append("CROSS ");
            break;
        default:
            throw new AssertionError(join.getType());
        }
    }

    public void visitOnClauses(JoinBase<T> join) {
        if (join.getOnClause() != null) {
            sb.append(" ON ");
            visit(join.getOnClause());
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
        if (!select.getGroupByExpressions().isEmpty()) {
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

    protected void appendCastType(AbstractCompoundDataType<?> compoundType) {
        switch (compoundType.getDataType().toString()) {
        case "BOOLEAN":
            mapBooleanType(compoundType);
            break;
        case "INT":
            mapIntType(compoundType);
            break;
        case "TEXT":
            mapTextType(compoundType);
            break;
        case "REAL":
            mapRealType(compoundType);
            break;
        case "DECIMAL":
            mapDecimalType(compoundType);
            break;
        case "FLOAT":
            mapFloatType(compoundType);
            break;
        case "BIT":
            mapBitType(compoundType);
            break;
        case "RANGE":
            mapRangeType(compoundType);
            break;
        case "MONEY":
            mapMoneyType(compoundType);
            break;
        case "INET":
            mapInetType(compoundType);
            break;
        case "BYTEA":
            mapByteaType(compoundType);
            break;
        default:
            throw new AssertionError(compoundType.getDataType());
        }

        Optional<Integer> size = compoundType.getSize();
        if (size.isPresent()) {
            sb.append("(");
            sb.append(size.get());
            sb.append(")");
        }
    }

    protected void mapBooleanType(AbstractCompoundDataType<?> compoundType) {
        sb.append("BOOLEAN");
    }

    protected void mapIntType(AbstractCompoundDataType<?> compoundType) {
        sb.append("INT");
    }

    protected void mapTextType(AbstractCompoundDataType<?> compoundType) {
        sb.append(Randomly.fromOptions("VARCHAR"));
    }

    protected void mapRealType(AbstractCompoundDataType<?> compoundType) {
        sb.append("REAL");
    }

    protected void mapDecimalType(AbstractCompoundDataType<?> compoundType) {
        sb.append("DECIMAL");
    }

    protected void mapFloatType(AbstractCompoundDataType<?> compoundType) {
        sb.append("FLOAT");
    }

    protected void mapBitType(AbstractCompoundDataType<?> compoundType) {
        sb.append("BIT");
    }

    protected void mapRangeType(AbstractCompoundDataType<?> compoundType) {
        sb.append("int4range");
    }

    protected void mapMoneyType(AbstractCompoundDataType<?> compoundType) {
        sb.append("MONEY");
    }

    protected void mapInetType(AbstractCompoundDataType<?> compoundType) {
        sb.append("INET");
    }

    protected void mapByteaType(AbstractCompoundDataType<?> compoundType) {
        sb.append("BYTEA");
    }

    protected void visitCastOperation(AbstractCastOperation<T, ?> cast) {
        if (Randomly.getBoolean()) {
            sb.append("CAST(");
            visit(cast.getExpression());
            sb.append(" AS ");
            appendType(cast);
            sb.append(")");
        } else {
            sb.append("(");
            visit(cast.getExpression());
            sb.append(")::");
            appendType(cast);
        }
    }

    protected void appendType(AbstractCastOperation<T, ?> cast) {
        appendCastType(cast.getCompoundType());
    }

    protected void visitUnaryPostfixOperation(AbstractPostfixOperation<T> op) {
        sb.append("(");
        visit(op.getExpression());
        sb.append(")");
        sb.append(" IS ");
        if (op.isNegated()) {
            sb.append("NOT ");
        }

        String operatorName = op.getOperator().toString();
        switch (operatorName) {
        case "IS_FALSE":
            sb.append("FALSE");
            break;
        case "IS_NULL":
            if (Randomly.getBoolean()) {
                sb.append("UNKNOWN");
            } else {
                sb.append("NULL");
            }
            break;
        case "IS_TRUE":
            sb.append("TRUE");
            break;
        default:
            throw new AssertionError(op);
        }
    }

    protected void visitBinaryLogicalOperation(AbstractBinaryLogicalOperation<T> op) {
        sb.append("(");
        visit(op.getLeft());
        sb.append(")");
        sb.append(" ");
        sb.append(op.getTextRepresentation());
        sb.append(" ");
        sb.append("(");
        visit(op.getRight());
        sb.append(")");
    }

    protected void visitInOperation(AbstractInOperation<T> op) {
        sb.append("(");
        visit(op.getExpr());
        sb.append(")");
        if (!op.isTrue()) {
            sb.append(" NOT");
        }
        sb.append(" IN ");
        sb.append("(");
        for (int i = 0; i < op.getListElements().size(); i++) {
            if (i != 0) {
                sb.append(", ");
            }
            visit(op.getListElements().get(i));
        }
        sb.append(")");
    }

    public void generateCaseStatement(T switchCondition, List<T> conditions, List<T> thenClauses,
                                       T elseExpression, boolean isCockroachDB) {
        if (isCockroachDB) {
            sb.append("CASE ");
        } else {
            sb.append("(CASE ");
            visit(switchCondition);
        }

        for (int i = 0; i < conditions.size(); i++) {
            sb.append(" WHEN ");
            visit(conditions.get(i));
            sb.append(" THEN ");
            visit(thenClauses.get(i));
            if (isCockroachDB) {
                sb.append(" ");
            }
        }

        if (elseExpression != null) {
            sb.append(" ELSE ");
            visit(elseExpression);
            if (isCockroachDB) {
                sb.append(" ");
            }
        }

        if (isCockroachDB) {
            sb.append("END");
        } else {
            sb.append(" END )");
        }
    }

}
