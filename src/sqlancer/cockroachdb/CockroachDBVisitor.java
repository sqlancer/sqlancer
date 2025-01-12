package sqlancer.cockroachdb;

import sqlancer.cockroachdb.ast.CockroachDBAggregate;
import sqlancer.cockroachdb.ast.CockroachDBAlias;
import sqlancer.cockroachdb.ast.CockroachDBAllOperator;
import sqlancer.cockroachdb.ast.CockroachDBAnyOperator;
import sqlancer.cockroachdb.ast.CockroachDBBetweenOperation;
import sqlancer.cockroachdb.ast.CockroachDBCaseOperation;
import sqlancer.cockroachdb.ast.CockroachDBColumnReference;
import sqlancer.cockroachdb.ast.CockroachDBConstant;
import sqlancer.cockroachdb.ast.CockroachDBExists;
import sqlancer.cockroachdb.ast.CockroachDBExpression;
import sqlancer.cockroachdb.ast.CockroachDBExpressionBag;
import sqlancer.cockroachdb.ast.CockroachDBFunctionCall;
import sqlancer.cockroachdb.ast.CockroachDBInOperation;
import sqlancer.cockroachdb.ast.CockroachDBJoin;
import sqlancer.cockroachdb.ast.CockroachDBMultiValuedComparison;
import sqlancer.cockroachdb.ast.CockroachDBResultMap;
import sqlancer.cockroachdb.ast.CockroachDBSelect;
import sqlancer.cockroachdb.ast.CockroachDBTableAndColumnReference;
import sqlancer.cockroachdb.ast.CockroachDBTableReference;
import sqlancer.cockroachdb.ast.CockroachDBTypeof;
import sqlancer.cockroachdb.ast.CockroachDBValues;
import sqlancer.cockroachdb.ast.CockroachDBWithClasure;

public interface CockroachDBVisitor {

    void visit(CockroachDBConstant c);

    void visit(CockroachDBColumnReference c);

    void visit(CockroachDBFunctionCall call);

    void visit(CockroachDBInOperation inOp);

    void visit(CockroachDBBetweenOperation op);

    void visit(CockroachDBSelect select);

    void visit(CockroachDBCaseOperation cases);

    void visit(CockroachDBJoin join);

    void visit(CockroachDBTableReference tableRef);

    void visit(CockroachDBAggregate aggr);

    void visit(CockroachDBMultiValuedComparison comp);

    // CODDTest
    void visit(CockroachDBExists existsExpr);
    void visit(CockroachDBExpressionBag exprBag);
    void visit(CockroachDBValues values);
    void visit(CockroachDBWithClasure withClasure);
    void visit(CockroachDBTableAndColumnReference tableAndColumnReference);
    void visit(CockroachDBAlias alias);
    void visit(CockroachDBTypeof typeOf);
    void visit(CockroachDBResultMap resMap);
    void visit(CockroachDBAllOperator allOperator);
    void visit(CockroachDBAnyOperator anyOperator);

    default void visit(CockroachDBExpression expr) {
        if (expr instanceof CockroachDBConstant) {
            visit((CockroachDBConstant) expr);
        } else if (expr instanceof CockroachDBColumnReference) {
            visit((CockroachDBColumnReference) expr);
        } else if (expr instanceof CockroachDBFunctionCall) {
            visit((CockroachDBFunctionCall) expr);
        } else if (expr instanceof CockroachDBInOperation) {
            visit((CockroachDBInOperation) expr);
        } else if (expr instanceof CockroachDBBetweenOperation) {
            visit((CockroachDBBetweenOperation) expr);
        } else if (expr instanceof CockroachDBSelect) {
            visit((CockroachDBSelect) expr);
        } else if (expr instanceof CockroachDBCaseOperation) {
            visit((CockroachDBCaseOperation) expr);
        } else if (expr instanceof CockroachDBJoin) {
            visit((CockroachDBJoin) expr);
        } else if (expr instanceof CockroachDBTableReference) {
            visit((CockroachDBTableReference) expr);
        } else if (expr instanceof CockroachDBAggregate) {
            visit((CockroachDBAggregate) expr);
        } else if (expr instanceof CockroachDBMultiValuedComparison) {
            visit((CockroachDBMultiValuedComparison) expr);
        } else if (expr instanceof CockroachDBExists) {
            visit((CockroachDBExists) expr);
        } else if (expr instanceof CockroachDBExpressionBag) {
            visit((CockroachDBExpressionBag) expr);
        } else if (expr instanceof CockroachDBValues) {
            visit((CockroachDBValues) expr);
        } else if (expr instanceof CockroachDBWithClasure) {
            visit ((CockroachDBWithClasure) expr);
        } else if (expr instanceof CockroachDBTableAndColumnReference) {
            visit ((CockroachDBTableAndColumnReference) expr);
        } else if (expr instanceof CockroachDBAlias) {
            visit ((CockroachDBAlias) expr);
        } else if (expr instanceof CockroachDBTypeof) {
            visit ((CockroachDBTypeof) expr);
        } else if (expr instanceof CockroachDBResultMap) {
            visit ((CockroachDBResultMap) expr);
        } else if (expr instanceof CockroachDBAllOperator) {
            visit((CockroachDBAllOperator) expr);
        } else if (expr instanceof CockroachDBAnyOperator) {
            visit((CockroachDBAnyOperator) expr);
        } else {
            throw new AssertionError(expr.getClass());
        }
    }

    static String asString(CockroachDBExpression expr) {
        CockroachDBToStringVisitor v = new CockroachDBToStringVisitor();
        v.visit(expr);
        return v.getString();
    }

}
