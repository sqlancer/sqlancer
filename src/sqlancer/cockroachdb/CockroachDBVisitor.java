package sqlancer.cockroachdb;

import sqlancer.cockroachdb.ast.CockroachDBAggregate;
import sqlancer.cockroachdb.ast.CockroachDBBetweenOperation;
import sqlancer.cockroachdb.ast.CockroachDBCaseOperation;
import sqlancer.cockroachdb.ast.CockroachDBColumnReference;
import sqlancer.cockroachdb.ast.CockroachDBConstant;
import sqlancer.cockroachdb.ast.CockroachDBExpression;
import sqlancer.cockroachdb.ast.CockroachDBFunctionCall;
import sqlancer.cockroachdb.ast.CockroachDBInOperation;
import sqlancer.cockroachdb.ast.CockroachDBJoin;
import sqlancer.cockroachdb.ast.CockroachDBMultiValuedComparison;
import sqlancer.cockroachdb.ast.CockroachDBSelect;
import sqlancer.cockroachdb.ast.CockroachDBTableReference;

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
