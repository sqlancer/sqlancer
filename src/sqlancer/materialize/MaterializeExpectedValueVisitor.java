package sqlancer.materialize;

import sqlancer.materialize.ast.MaterializeAggregate;
import sqlancer.materialize.ast.MaterializeBetweenOperation;
import sqlancer.materialize.ast.MaterializeBinaryLogicalOperation;
import sqlancer.materialize.ast.MaterializeCastOperation;
import sqlancer.materialize.ast.MaterializeColumnValue;
import sqlancer.materialize.ast.MaterializeConstant;
import sqlancer.materialize.ast.MaterializeExpression;
import sqlancer.materialize.ast.MaterializeFunction;
import sqlancer.materialize.ast.MaterializeInOperation;
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

public final class MaterializeExpectedValueVisitor implements MaterializeVisitor {

    private final StringBuilder sb = new StringBuilder();
    private static final int NR_TABS = 0;

    private void print(MaterializeExpression expr) {
        MaterializeToStringVisitor v = new MaterializeToStringVisitor();
        v.visit(expr);
        for (int i = 0; i < NR_TABS; i++) {
            sb.append("\t");
        }
        sb.append(v.get());
        sb.append(" -- ");
        sb.append(expr.getExpectedValue());
        sb.append("\n");
    }

    @Override
    public void visit(MaterializeConstant constant) {
        print(constant);
    }

    @Override
    public void visit(MaterializePostfixOperation op) {
        print(op);
        visit(op.getExpression());
    }

    public String get() {
        return sb.toString();
    }

    @Override
    public void visit(MaterializeColumnValue c) {
        print(c);
    }

    @Override
    public void visit(MaterializePrefixOperation op) {
        print(op);
        visit(op.getExpression());
    }

    @Override
    public void visit(MaterializeSelect op) {
        visit(op.getWhereClause());
    }

    @Override
    public void visit(MaterializeOrderByTerm op) {

    }

    @Override
    public void visit(MaterializeFunction f) {
        print(f);
        for (int i = 0; i < f.getArguments().length; i++) {
            visit(f.getArguments()[i]);
        }
    }

    @Override
    public void visit(MaterializeCastOperation cast) {
        print(cast);
        visit(cast.getExpression());
    }

    @Override
    public void visit(MaterializeBetweenOperation op) {
        print(op);
        visit(op.getExpr());
        visit(op.getLeft());
        visit(op.getRight());
    }

    @Override
    public void visit(MaterializeInOperation op) {
        print(op);
        visit(op.getExpr());
        for (MaterializeExpression right : op.getListElements()) {
            visit(right);
        }
    }

    @Override
    public void visit(MaterializePostfixText op) {
        print(op);
        visit(op.getExpr());
    }

    @Override
    public void visit(MaterializeAggregate op) {
        print(op);
        for (MaterializeExpression expr : op.getArgs()) {
            visit(expr);
        }
    }

    @Override
    public void visit(MaterializeSimilarTo op) {
        print(op);
        visit(op.getString());
        visit(op.getSimilarTo());
        if (op.getEscapeCharacter() != null) {
            visit(op.getEscapeCharacter());
        }
    }

    @Override
    public void visit(MaterializePOSIXRegularExpression op) {
        print(op);
        visit(op.getString());
        visit(op.getRegex());
    }

    @Override
    public void visit(MaterializeFromTable from) {
        print(from);
    }

    @Override
    public void visit(MaterializeSubquery subquery) {
        print(subquery);
    }

    @Override
    public void visit(MaterializeBinaryLogicalOperation op) {
        print(op);
        visit(op.getLeft());
        visit(op.getRight());
    }

    @Override
    public void visit(MaterializeLikeOperation op) {
        print(op);
        visit(op.getLeft());
        visit(op.getRight());
    }

}
