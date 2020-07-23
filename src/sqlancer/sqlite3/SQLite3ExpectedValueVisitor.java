package sqlancer.sqlite3;

import sqlancer.sqlite3.ast.SQLite3Aggregate;
import sqlancer.sqlite3.ast.SQLite3Case.CasePair;
import sqlancer.sqlite3.ast.SQLite3Case.SQLite3CaseWithBaseExpression;
import sqlancer.sqlite3.ast.SQLite3Case.SQLite3CaseWithoutBaseExpression;
import sqlancer.sqlite3.ast.SQLite3Constant;
import sqlancer.sqlite3.ast.SQLite3Expression;
import sqlancer.sqlite3.ast.SQLite3Expression.BetweenOperation;
import sqlancer.sqlite3.ast.SQLite3Expression.BinaryComparisonOperation;
import sqlancer.sqlite3.ast.SQLite3Expression.Cast;
import sqlancer.sqlite3.ast.SQLite3Expression.CollateOperation;
import sqlancer.sqlite3.ast.SQLite3Expression.Function;
import sqlancer.sqlite3.ast.SQLite3Expression.InOperation;
import sqlancer.sqlite3.ast.SQLite3Expression.Join;
import sqlancer.sqlite3.ast.SQLite3Expression.MatchOperation;
import sqlancer.sqlite3.ast.SQLite3Expression.SQLite3ColumnName;
import sqlancer.sqlite3.ast.SQLite3Expression.SQLite3Distinct;
import sqlancer.sqlite3.ast.SQLite3Expression.SQLite3Exist;
import sqlancer.sqlite3.ast.SQLite3Expression.SQLite3OrderingTerm;
import sqlancer.sqlite3.ast.SQLite3Expression.SQLite3PostfixText;
import sqlancer.sqlite3.ast.SQLite3Expression.SQLite3PostfixUnaryOperation;
import sqlancer.sqlite3.ast.SQLite3Expression.SQLite3TableReference;
import sqlancer.sqlite3.ast.SQLite3Expression.SQLite3Text;
import sqlancer.sqlite3.ast.SQLite3Expression.Sqlite3BinaryOperation;
import sqlancer.sqlite3.ast.SQLite3Expression.Subquery;
import sqlancer.sqlite3.ast.SQLite3Expression.TypeLiteral;
import sqlancer.sqlite3.ast.SQLite3Function;
import sqlancer.sqlite3.ast.SQLite3RowValueExpression;
import sqlancer.sqlite3.ast.SQLite3Select;
import sqlancer.sqlite3.ast.SQLite3SetClause;
import sqlancer.sqlite3.ast.SQLite3UnaryOperation;
import sqlancer.sqlite3.ast.SQLite3WindowFunction;
import sqlancer.sqlite3.ast.SQLite3WindowFunctionExpression;
import sqlancer.sqlite3.ast.SQLite3WindowFunctionExpression.SQLite3WindowFunctionFrameSpecBetween;
import sqlancer.sqlite3.ast.SQLite3WindowFunctionExpression.SQLite3WindowFunctionFrameSpecTerm;

public class SQLite3ExpectedValueVisitor implements SQLite3Visitor {

    private final StringBuilder sb = new StringBuilder();
    private int nrTabs;

    private void print(SQLite3Expression expr) {
        SQLite3ToStringVisitor v = new SQLite3ToStringVisitor();
        v.visit(expr);
        for (int i = 0; i < nrTabs; i++) {
            sb.append("\t");
        }
        sb.append(v.get());
        sb.append(" -- ");
        sb.append(expr.getExpectedValue());
        sb.append(" explicit collate: ");
        sb.append(expr.getExplicitCollateSequence());
        sb.append(" implicit collate: ");
        sb.append(expr.getImplicitCollateSequence());
        sb.append("\n");
    }

    @Override
    public void visit(SQLite3Expression expr) {
        nrTabs++;
        SQLite3Visitor.super.visit(expr);
        nrTabs--;
    }

    @Override
    public void visit(Sqlite3BinaryOperation op) {
        print(op);
        visit(op.getLeft());
        visit(op.getRight());
    }

    @Override
    public void visit(BetweenOperation op) {
        print(op);
        visit(op.getTopNode());
    }

    @Override
    public void visit(SQLite3ColumnName c) {
        print(c);
    }

    @Override
    public void visit(SQLite3Constant c) {
        print(c);
    }

    @Override
    public void visit(Function f) {
        print(f);
        for (SQLite3Expression expr : f.getArguments()) {
            visit(expr);
        }
    }

    @Override
    public void visit(SQLite3Select s, boolean inner) {
        for (SQLite3Expression expr : s.getFetchColumns()) {
            if (expr instanceof SQLite3Aggregate) {
                visit(expr);
            }
        }
        for (SQLite3Expression expr : s.getJoinClauses()) {
            visit(expr);
        }
        visit(s.getWhereClause());
        if (s.getHavingClause() != null) {
            visit(s.getHavingClause());
        }
    }

    @Override
    public void visit(SQLite3OrderingTerm term) {
        sb.append("(");
        print(term);
        visit(term.getExpression());
        sb.append(")");
    }

    @Override
    public void visit(SQLite3UnaryOperation exp) {
        print(exp);
        visit(exp.getExpression());
    }

    @Override
    public void visit(SQLite3PostfixUnaryOperation exp) {
        print(exp);
        visit(exp.getExpression());
    }

    @Override
    public void visit(CollateOperation op) {
        print(op);
        visit(op.getExpression());
    }

    @Override
    public void visit(Cast cast) {
        print(cast);
        visit(cast.getExpression());
    }

    @Override
    public void visit(TypeLiteral literal) {
    }

    @Override
    public void visit(InOperation op) {
        print(op);
        visit(op.getLeft());
        if (op.getRightExpressionList() != null) {
            for (SQLite3Expression expr : op.getRightExpressionList()) {
                visit(expr);
            }
        } else {
            visit(op.getRightSelect());
        }
    }

    @Override
    public void visit(Subquery query) {
        print(query);
        if (query.getExpectedValue() != null) {
            visit(query.getExpectedValue());
        }
    }

    @Override
    public void visit(SQLite3Exist exist) {
        print(exist);
        visit(exist.getExpression());
    }

    @Override
    public void visit(Join join) {
        print(join);
        visit(join.getOnClause());
    }

    @Override
    public void visit(BinaryComparisonOperation op) {
        print(op);
        visit(op.getLeft());
        visit(op.getRight());
    }

    public String get() {
        return sb.toString();
    }

    @Override
    public void visit(SQLite3Function func) {
        print(func);
        for (SQLite3Expression expr : func.getArgs()) {
            visit(expr);
        }
    }

    @Override
    public void visit(SQLite3Distinct distinct) {
        print(distinct);
        visit(distinct.getExpression());
    }

    @Override
    public void visit(SQLite3CaseWithoutBaseExpression caseExpr) {
        for (CasePair cExpr : caseExpr.getPairs()) {
            print(cExpr.getCond());
            visit(cExpr.getCond());
            print(cExpr.getThen());
            visit(cExpr.getThen());
        }
        if (caseExpr.getElseExpr() != null) {
            print(caseExpr.getElseExpr());
            visit(caseExpr.getElseExpr());
        }
    }

    @Override
    public void visit(SQLite3CaseWithBaseExpression caseExpr) {
        print(caseExpr);
        visit(caseExpr.getBaseExpr());
        for (CasePair cExpr : caseExpr.getPairs()) {
            print(cExpr.getCond());
            visit(cExpr.getCond());
            print(cExpr.getThen());
            visit(cExpr.getThen());
        }
        if (caseExpr.getElseExpr() != null) {
            print(caseExpr.getElseExpr());
            visit(caseExpr.getElseExpr());
        }
    }

    @Override
    public void visit(SQLite3Aggregate aggr) {
        print(aggr);
        visit(aggr.getExpectedValue());
    }

    @Override
    public void visit(SQLite3PostfixText op) {
        print(op);
        if (op.getExpression() != null) {
            visit(op.getExpression());
        }
    }

    @Override
    public void visit(SQLite3WindowFunction func) {
        print(func);
        for (SQLite3Expression expr : func.getArgs()) {
            visit(expr);
        }
    }

    @Override
    public void visit(MatchOperation match) {
        print(match);
        visit(match.getLeft());
        visit(match.getRight());
    }

    @Override
    public void visit(SQLite3RowValueExpression rw) {
        print(rw);
        for (SQLite3Expression expr : rw.getExpressions()) {
            visit(expr);
        }
    }

    @Override
    public void visit(SQLite3Text func) {
        print(func);
    }

    @Override
    public void visit(SQLite3WindowFunctionExpression windowFunction) {

    }

    @Override
    public void visit(SQLite3WindowFunctionFrameSpecTerm term) {

    }

    @Override
    public void visit(SQLite3WindowFunctionFrameSpecBetween between) {

    }

    @Override
    public void visit(SQLite3TableReference tableReference) {

    }

    @Override
    public void visit(SQLite3SetClause set) {
        print(set);
        visit(set.getLeft());
        visit(set.getRight());
    }

}
