package sqlancer.postgres;

import sqlancer.Randomly;
import sqlancer.common.ast.JoinBase;
import sqlancer.common.schema.AbstractCompoundDataType;
import sqlancer.common.visitor.BinaryOperation;
import sqlancer.common.visitor.ToStringVisitor;
import sqlancer.postgres.PostgresSchema.PostgresDataType;
import sqlancer.postgres.ast.PostgresAggregate;
import sqlancer.postgres.ast.PostgresBetweenOperation;
import sqlancer.postgres.ast.PostgresBinaryLogicalOperation;
import sqlancer.postgres.ast.PostgresCastOperation;
import sqlancer.postgres.ast.PostgresCollate;
import sqlancer.postgres.ast.PostgresColumnReference;
import sqlancer.postgres.ast.PostgresColumnValue;
import sqlancer.postgres.ast.PostgresConstant;
import sqlancer.postgres.ast.PostgresExpression;
import sqlancer.postgres.ast.PostgresFunction;
import sqlancer.postgres.ast.PostgresInOperation;
import sqlancer.postgres.ast.PostgresLikeOperation;
import sqlancer.postgres.ast.PostgresOrderByTerm;
import sqlancer.postgres.ast.PostgresPOSIXRegularExpression;
import sqlancer.postgres.ast.PostgresPostfixOperation;
import sqlancer.postgres.ast.PostgresPostfixText;
import sqlancer.postgres.ast.PostgresPrefixOperation;
import sqlancer.postgres.ast.PostgresSelect;
import sqlancer.postgres.ast.PostgresSelect.PostgresFromTable;
import sqlancer.postgres.ast.PostgresSelect.PostgresSubquery;
import sqlancer.postgres.ast.PostgresSimilarTo;
import sqlancer.postgres.ast.PostgresTableReference;

public final class PostgresToStringVisitor extends ToStringVisitor<PostgresExpression> implements PostgresVisitor {

    @Override
    protected boolean hasDistinctOnSupport() {
        return true;
    }

    @Override
    public void visitSpecific(PostgresExpression expr) {
        PostgresVisitor.super.visit(expr);
    }

    @Override
    public void visit(PostgresConstant constant) {
        sb.append(constant.getTextRepresentation());
    }

    @Override
    public String get() {
        return sb.toString();
    }

    @Override
    public void visit(PostgresColumnReference column) {
        sb.append(column.getColumn().getFullQualifiedName());
    }

    @Override
    public void visit(PostgresPostfixOperation op) {
        sb.append("(");
        visit(op.getExpression());
        sb.append(")");
        sb.append(" ");
        sb.append(op.getOperatorTextRepresentation());
    }

    @Override
    public void visit(PostgresColumnValue c) {
        sb.append(c.getColumn().getFullQualifiedName());
    }

    @Override
    public void visit(PostgresPrefixOperation op) {
        sb.append(op.getTextRepresentation());
        sb.append(" (");
        visit(op.getExpression());
        sb.append(")");
    }

    @Override
    public void visit(PostgresFromTable from) {
        if (from.isOnly()) {
            sb.append("ONLY ");
        }
        sb.append(from.getTable().getName());
        if (!from.isOnly() && Randomly.getBoolean()) {
            sb.append("*");
        }
    }

    @Override
    public void visit(PostgresSubquery subquery) {
        sb.append("(");
        visit(subquery.getSelect());
        sb.append(") AS ");
        sb.append(subquery.getName());
    }

    @Override
    public void visit(PostgresTableReference ref) {
        sb.append(ref.getTable().getName());
    }

    @Override
    public void visit(PostgresSelect s) {
        visitSelect(s);
    }

    @Override
    protected PostgresExpression getJoinOnClause(JoinBase<PostgresExpression> join) {
        return join.getOnClause();
    }

    @Override
    protected PostgresExpression getJoinTableReference(JoinBase<PostgresExpression> join) {
        return join.getTableReference();
    }

    @Override
    public void visit(PostgresOrderByTerm op) {
        visit(op.getExpr());
        sb.append(" ");
        sb.append(op.getOrder());
    }

    @Override
    public void visit(PostgresFunction f) {
        sb.append(f.getFunctionName());
        sb.append("(");
        int i = 0;
        for (PostgresExpression arg : f.getArguments()) {
            if (i++ != 0) {
                sb.append(", ");
            }
            visit(arg);
        }
        sb.append(")");
    }

    @Override
    public void visit(PostgresCastOperation cast) {
        visitCastOperation(cast);
    }

    @Override
    public void mapRealType(AbstractCompoundDataType<?> compoundType) {
        sb.append("FLOAT");
    }

    @Override
    public void mapFloatType(AbstractCompoundDataType<?> compoundType) {
        sb.append("REAL");
    }

    @Override
    public void mapByteaType(AbstractCompoundDataType<?> compoundType) {
        throw new AssertionError(compoundType.getDataType());
    }

    @Override
    public void visit(PostgresBetweenOperation op) {
        sb.append("(");
        visit(op.getExpr());
        if (PostgresProvider.generateOnlyKnown && op.getExpr().getExpressionType() == PostgresDataType.TEXT
                && op.getLeft().getExpressionType() == PostgresDataType.TEXT) {
            sb.append(" COLLATE \"C\"");
        }
        sb.append(") BETWEEN ");
        if (op.isSymmetric()) {
            sb.append("SYMMETRIC ");
        }
        sb.append("(");
        visit(op.getLeft());
        sb.append(") AND (");
        visit(op.getRight());
        if (PostgresProvider.generateOnlyKnown && op.getExpr().getExpressionType() == PostgresDataType.TEXT
                && op.getRight().getExpressionType() == PostgresDataType.TEXT) {
            sb.append(" COLLATE \"C\"");
        }
        sb.append(")");
    }

    @Override
    public void visit(PostgresInOperation op) {
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
    public void visit(PostgresPostfixText op) {
        visit(op.getExpr());
        sb.append(op.getText());
    }

    @Override
    public void visit(PostgresAggregate op) {
        sb.append(op.getFunction());
        sb.append("(");
        visit(op.getArgs());
        sb.append(")");
    }

    @Override
    public void visit(PostgresSimilarTo op) {
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
    public void visit(PostgresPOSIXRegularExpression op) {
        visit(op.getString());
        sb.append(op.getOp().getStringRepresentation());
        visit(op.getRegex());
    }

    @Override
    public void visit(PostgresCollate op) {
        sb.append("(");
        visit(op.getExpr());
        sb.append(" COLLATE ");
        sb.append('"');
        sb.append(op.getCollate());
        sb.append('"');
        sb.append(")");
    }

    @Override
    public void visit(PostgresBinaryLogicalOperation op) {
        super.visit((BinaryOperation<PostgresExpression>) op);
    }

    @Override
    public void visit(PostgresLikeOperation op) {
        super.visit((BinaryOperation<PostgresExpression>) op);
    }

}
