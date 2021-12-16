package sqlancer.oceanbase;

import sqlancer.IgnoreMeException;
import sqlancer.oceanbase.ast.OceanBaseAggregate;
import sqlancer.oceanbase.ast.OceanBaseBinaryComparisonOperation;
import sqlancer.oceanbase.ast.OceanBaseBinaryLogicalOperation;
import sqlancer.oceanbase.ast.OceanBaseCastOperation;
import sqlancer.oceanbase.ast.OceanBaseColumnName;
import sqlancer.oceanbase.ast.OceanBaseColumnReference;
import sqlancer.oceanbase.ast.OceanBaseComputableFunction;
import sqlancer.oceanbase.ast.OceanBaseConstant;
import sqlancer.oceanbase.ast.OceanBaseExists;
import sqlancer.oceanbase.ast.OceanBaseExpression;
import sqlancer.oceanbase.ast.OceanBaseInOperation;
import sqlancer.oceanbase.ast.OceanBaseOrderByTerm;
import sqlancer.oceanbase.ast.OceanBaseSelect;
import sqlancer.oceanbase.ast.OceanBaseStringExpression;
import sqlancer.oceanbase.ast.OceanBaseTableReference;
import sqlancer.oceanbase.ast.OceanBaseText;
import sqlancer.oceanbase.ast.OceanBaseUnaryPostfixOperation;
import sqlancer.oceanbase.ast.OceanBaseUnaryPrefixOperation;

public class OceanBaseExpectedValueVisitor implements OceanBaseVisitor {

    private final StringBuilder sb = new StringBuilder();
    private int nrTabs;

    private void print(OceanBaseExpression expr) {
        OceanBaseToStringVisitor v = new OceanBaseToStringVisitor();
        v.visit(expr);
        for (int i = 0; i < nrTabs; i++) {
            sb.append("\t");
        }
        sb.append(v.get());
        sb.append(" -- ");
        sb.append(expr.getExpectedValue());
        sb.append("\n");
    }

    @Override
    public void visit(OceanBaseExpression expr) {
        nrTabs++;
        try {
            OceanBaseVisitor.super.visit(expr);
        } catch (IgnoreMeException e) {

        }
        nrTabs--;
    }

    @Override
    public void visit(OceanBaseConstant constant) {
        print(constant);
    }

    @Override
    public void visit(OceanBaseColumnReference column) {
        print(column);
    }

    @Override
    public void visit(OceanBaseUnaryPostfixOperation op) {
        print(op);
        visit(op.getExpression());
    }

    @Override
    public void visit(OceanBaseComputableFunction f) {
        print(f);
        for (OceanBaseExpression expr : f.getArguments()) {
            visit(expr);
        }
    }

    @Override
    public void visit(OceanBaseBinaryLogicalOperation op) {
        print(op);
        visit(op.getLeft());
        visit(op.getRight());
    }

    public String get() {
        return sb.toString();
    }

    @Override
    public void visit(OceanBaseSelect select) {
        for (OceanBaseExpression j : select.getJoinList()) {
            visit(j);
        }
        if (select.getWhereClause() != null) {
            visit(select.getWhereClause());
        }
    }

    @Override
    public void visit(OceanBaseBinaryComparisonOperation op) {
        print(op);
        visit(op.getLeft());
        visit(op.getRight());
    }

    @Override
    public void visit(OceanBaseCastOperation op) {
        print(op);
        visit(op.getExpr());
    }

    @Override
    public void visit(OceanBaseInOperation op) {
        print(op);
        visit(op.getExpr());
        for (OceanBaseExpression right : op.getListElements()) {
            visit(right);
        }
    }

    @Override
    public void visit(OceanBaseOrderByTerm op) {
    }

    @Override
    public void visit(OceanBaseExists op) {
        print(op);
        visit(op.getExpr());
    }

    @Override
    public void visit(OceanBaseStringExpression op) {
        print(op);
    }

    @Override
    public void visit(OceanBaseTableReference ref) {
    }

    @Override
    public void visit(OceanBaseAggregate aggr) {
    }

    @Override
    public void visit(OceanBaseColumnName aggr) {
    }

    @Override
    public void visit(OceanBaseText func) {
    }

    @Override
    public void visit(OceanBaseUnaryPrefixOperation op) {
        print(op);
        visit(op.getExpr());
    }

}
