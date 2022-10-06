package sqlancer.yugabyte.ysql;

import sqlancer.yugabyte.ysql.ast.YSQLAggregate;
import sqlancer.yugabyte.ysql.ast.YSQLBetweenOperation;
import sqlancer.yugabyte.ysql.ast.YSQLBinaryLogicalOperation;
import sqlancer.yugabyte.ysql.ast.YSQLCastOperation;
import sqlancer.yugabyte.ysql.ast.YSQLColumnValue;
import sqlancer.yugabyte.ysql.ast.YSQLConstant;
import sqlancer.yugabyte.ysql.ast.YSQLExpression;
import sqlancer.yugabyte.ysql.ast.YSQLFunction;
import sqlancer.yugabyte.ysql.ast.YSQLInOperation;
import sqlancer.yugabyte.ysql.ast.YSQLOrderByTerm;
import sqlancer.yugabyte.ysql.ast.YSQLPOSIXRegularExpression;
import sqlancer.yugabyte.ysql.ast.YSQLPostfixOperation;
import sqlancer.yugabyte.ysql.ast.YSQLPostfixText;
import sqlancer.yugabyte.ysql.ast.YSQLPrefixOperation;
import sqlancer.yugabyte.ysql.ast.YSQLSelect;
import sqlancer.yugabyte.ysql.ast.YSQLSelect.YSQLFromTable;
import sqlancer.yugabyte.ysql.ast.YSQLSelect.YSQLSubquery;
import sqlancer.yugabyte.ysql.ast.YSQLSimilarTo;

public final class YSQLExpectedValueVisitor implements YSQLVisitor {

    private static final int NR_TABS = 0;
    private final StringBuilder sb = new StringBuilder();

    private void print(YSQLExpression expr) {
        YSQLToStringVisitor v = new YSQLToStringVisitor();
        v.visit(expr);
        sb.append("\t".repeat(NR_TABS));
        sb.append(v.get());
        sb.append(" -- ");
        sb.append(expr.getExpectedValue());
        sb.append("\n");
    }

    @Override
    public void visit(YSQLConstant constant) {
        print(constant);
    }

    @Override
    public void visit(YSQLPostfixOperation op) {
        print(op);
        visit(op.getExpression());
    }

    @Override
    public void visit(YSQLColumnValue c) {
        print(c);
    }

    @Override
    public void visit(YSQLPrefixOperation op) {
        print(op);
        visit(op.getExpression());
    }

    @Override
    public void visit(YSQLSelect op) {
        visit(op.getWhereClause());
    }

    @Override
    public void visit(YSQLOrderByTerm op) {

    }

    @Override
    public void visit(YSQLFunction f) {
        print(f);
        for (int i = 0; i < f.getArguments().length; i++) {
            visit(f.getArguments()[i]);
        }
    }

    @Override
    public void visit(YSQLCastOperation cast) {
        print(cast);
        visit(cast.getExpression());
    }

    @Override
    public void visit(YSQLBetweenOperation op) {
        print(op);
        visit(op.getExpr());
        visit(op.getLeft());
        visit(op.getRight());
    }

    @Override
    public void visit(YSQLInOperation op) {
        print(op);
        visit(op.getExpr());
        for (YSQLExpression right : op.getListElements()) {
            visit(right);
        }
    }

    @Override
    public void visit(YSQLPostfixText op) {
        print(op);
        visit(op.getExpr());
    }

    @Override
    public void visit(YSQLAggregate op) {
        print(op);
        for (YSQLExpression expr : op.getArgs()) {
            visit(expr);
        }
    }

    @Override
    public void visit(YSQLSimilarTo op) {
        print(op);
        visit(op.getString());
        visit(op.getSimilarTo());
        if (op.getEscapeCharacter() != null) {
            visit(op.getEscapeCharacter());
        }
    }

    @Override
    public void visit(YSQLPOSIXRegularExpression op) {
        print(op);
        visit(op.getString());
        visit(op.getRegex());
    }

    @Override
    public void visit(YSQLFromTable from) {
        print(from);
    }

    @Override
    public void visit(YSQLSubquery subquery) {
        print(subquery);
    }

    @Override
    public void visit(YSQLBinaryLogicalOperation op) {
        print(op);
        visit(op.getLeft());
        visit(op.getRight());
    }

    public String get() {
        return sb.toString();
    }

}
