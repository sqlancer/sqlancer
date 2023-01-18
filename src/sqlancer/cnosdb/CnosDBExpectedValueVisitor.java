package sqlancer.cnosdb;

import sqlancer.cnosdb.ast.*;
import sqlancer.cnosdb.ast.CnosDBSelect.CnosDBFromTable;
import sqlancer.cnosdb.ast.CnosDBSelect.CnosDBSubquery;

public final class CnosDBExpectedValueVisitor implements CnosDBVisitor {

    private final StringBuilder sb = new StringBuilder();
    private static final int NR_TABS = 0;

    private void print(CnosDBExpression expr) {
        CnosDBToStringVisitor v = new CnosDBToStringVisitor();
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
    public void visit(CnosDBConstant constant) {
        print(constant);
    }

    @Override
    public void visit(CnosDBPostfixOperation op) {
        print(op);
        visit(op.getExpression());
    }

    public String get() {
        return sb.toString();
    }

    @Override
    public void visit(CnosDBColumnValue c) {
        print(c);
    }

    @Override
    public void visit(CnosDBPrefixOperation op) {
        print(op);
        visit(op.getExpression());
    }

    @Override
    public void visit(CnosDBSelect op) {
        visit(op.getWhereClause());
    }

    @Override
    public void visit(CnosDBOrderByTerm op) {

    }

    @Override
    public void visit(CnosDBSimilarTo op) {
        print(op);
        visit(op.getString());
        visit(op.getSimilarTo());
        if (op.getEscapeCharacter() != null) {
            visit(op.getEscapeCharacter());
        }
    }

    @Override
    public void visit(CnosDBFunction f) {
        print(f);
        for (int i = 0; i < f.getArguments().length; i++) {
            visit(f.getArguments()[i]);
        }
    }

    @Override
    public void visit(CnosDBCastOperation cast) {
        print(cast);
        visit(cast.getExpression());
    }

    @Override
    public void visit(CnosDBBetweenOperation op) {
        print(op);
        visit(op.getExpr());
        visit(op.getLeft());
        visit(op.getRight());
    }

    @Override
    public void visit(CnosDBInOperation op) {
        print(op);
        visit(op.getExpr());
        for (CnosDBExpression right : op.getListElements()) {
            visit(right);
        }
    }

    @Override
    public void visit(CnosDBPostfixText op) {
        print(op);
        visit(op.getExpr());
    }

    @Override
    public void visit(CnosDBAggregate op) {
        print(op);
        for (CnosDBExpression expr : op.getArgs()) {
            visit(expr);
        }
    }

    @Override
    public void visit(CnosDBFromTable from) {
        print(from);
    }

    @Override
    public void visit(CnosDBSubquery subquery) {
        print(subquery);
    }

    @Override
    public void visit(CnosDBBinaryLogicalOperation op) {
        print(op);
        visit(op.getLeft());
        visit(op.getRight());
    }

    @Override
    public void visit(CnosDBLikeOperation op) {
        print(op);
        visit(op.getLeft());
        visit(op.getRight());
    }

}
