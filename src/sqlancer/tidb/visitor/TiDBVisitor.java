package sqlancer.tidb.visitor;

import sqlancer.tidb.ast.TiDBAggregate;
import sqlancer.tidb.ast.TiDBAlias;
import sqlancer.tidb.ast.TiDBAllOperator;
import sqlancer.tidb.ast.TiDBAnyOperator;
import sqlancer.tidb.ast.TiDBCase;
import sqlancer.tidb.ast.TiDBCastOperation;
import sqlancer.tidb.ast.TiDBColumnReference;
import sqlancer.tidb.ast.TiDBConstant;
import sqlancer.tidb.ast.TiDBExists;
import sqlancer.tidb.ast.TiDBExpression;
import sqlancer.tidb.ast.TiDBExpressionBag;
import sqlancer.tidb.ast.TiDBFunctionCall;
import sqlancer.tidb.ast.TiDBInOperator;
import sqlancer.tidb.ast.TiDBJoin;
import sqlancer.tidb.ast.TiDBResultMap;
import sqlancer.tidb.ast.TiDBSelect;
import sqlancer.tidb.ast.TiDBTableAndColumnReference;
import sqlancer.tidb.ast.TiDBTableReference;
import sqlancer.tidb.ast.TiDBText;
import sqlancer.tidb.ast.TiDBValues;
import sqlancer.tidb.ast.TiDBValuesRow;
import sqlancer.tidb.ast.TiDBWithClause;

public interface TiDBVisitor {

    default void visit(TiDBExpression expr) {
        if (expr instanceof TiDBConstant) {
            visit((TiDBConstant) expr);
        } else if (expr instanceof TiDBColumnReference) {
            visit((TiDBColumnReference) expr);
        } else if (expr instanceof TiDBSelect) {
            visit((TiDBSelect) expr);
        } else if (expr instanceof TiDBTableReference) {
            visit((TiDBTableReference) expr);
        } else if (expr instanceof TiDBFunctionCall) {
            visit((TiDBFunctionCall) expr);
        } else if (expr instanceof TiDBJoin) {
            visit((TiDBJoin) expr);
        } else if (expr instanceof TiDBText) {
            visit((TiDBText) expr);
        } else if (expr instanceof TiDBAggregate) {
            visit((TiDBAggregate) expr);
        } else if (expr instanceof TiDBCastOperation) {
            visit((TiDBCastOperation) expr);
        } else if (expr instanceof TiDBCase) {
            visit((TiDBCase) expr);
        } else if (expr instanceof TiDBAlias) {
            visit((TiDBAlias) expr);
        } else if (expr instanceof TiDBExists) {
            visit((TiDBExists) expr);
        } else if (expr instanceof TiDBExpressionBag) {
            visit((TiDBExpressionBag) expr);
        } else if (expr instanceof TiDBInOperator) {
            visit((TiDBInOperator) expr);
        } else if (expr instanceof TiDBTableAndColumnReference) {
            visit((TiDBTableAndColumnReference) expr);
        } else if (expr instanceof TiDBValues) {
            visit((TiDBValues) expr);
        } else if (expr instanceof TiDBValuesRow) {
            visit((TiDBValuesRow) expr);
        } else if (expr instanceof TiDBWithClause) {
            visit((TiDBWithClause) expr);
        } else if (expr instanceof TiDBResultMap) {
            visit((TiDBResultMap) expr);
        } else if (expr instanceof TiDBAllOperator) {
            visit((TiDBAllOperator) expr);
        } else if (expr instanceof TiDBAnyOperator) {
            visit((TiDBAnyOperator) expr);
        } else {
            throw new AssertionError(expr.getClass());
        }
    }

    void visit(TiDBCase caseExpr);

    void visit(TiDBCastOperation cast);

    void visit(TiDBAggregate aggr);

    void visit(TiDBFunctionCall call);

    void visit(TiDBConstant expr);

    void visit(TiDBColumnReference expr);

    void visit(TiDBTableReference expr);

    void visit(TiDBSelect select);

    void visit(TiDBJoin join);

    void visit(TiDBText text);

    // CODDTest
    void visit(TiDBAlias alias);
    void visit(TiDBExists exists);
    void visit(TiDBExpressionBag exprBag);
    void visit(TiDBInOperator inOperation);
    void visit(TiDBTableAndColumnReference tAndCRef);
    void visit(TiDBValues values);
    void visit(TiDBValuesRow values);
    void visit(TiDBWithClause withClause);
    void visit(TiDBResultMap tableSummary);
    void visit(TiDBAllOperator allOperation);
    void visit(TiDBAnyOperator anyOperation);

    static String asString(TiDBExpression expr) {
        TiDBToStringVisitor v = new TiDBToStringVisitor();
        v.visit(expr);
        return v.getString();
    }

}
