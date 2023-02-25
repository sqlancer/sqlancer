package sqlancer.cnosdb;

import sqlancer.cnosdb.ast.CnosDBAggregate;
import sqlancer.cnosdb.ast.CnosDBBetweenOperation;
import sqlancer.cnosdb.ast.CnosDBBinaryLogicalOperation;
import sqlancer.cnosdb.ast.CnosDBCastOperation;
import sqlancer.cnosdb.ast.CnosDBColumnValue;
import sqlancer.cnosdb.ast.CnosDBConstant;
import sqlancer.cnosdb.ast.CnosDBExpression;
import sqlancer.cnosdb.ast.CnosDBFunction;
import sqlancer.cnosdb.ast.CnosDBInOperation;
import sqlancer.cnosdb.ast.CnosDBLikeOperation;
import sqlancer.cnosdb.ast.CnosDBOrderByTerm;
import sqlancer.cnosdb.ast.CnosDBPostfixOperation;
import sqlancer.cnosdb.ast.CnosDBPostfixText;
import sqlancer.cnosdb.ast.CnosDBPrefixOperation;
import sqlancer.cnosdb.ast.CnosDBSelect;
import sqlancer.cnosdb.ast.CnosDBSelect.CnosDBFromTable;
import sqlancer.cnosdb.ast.CnosDBSelect.CnosDBSubquery;
import sqlancer.cnosdb.ast.CnosDBSimilarTo;

public interface CnosDBVisitor {

    static String asString(CnosDBExpression expr) {
        CnosDBToStringVisitor visitor = new CnosDBToStringVisitor();
        visitor.visit(expr);
        return visitor.get();
    }

    void visit(CnosDBConstant constant);

    void visit(CnosDBPostfixOperation op);

    void visit(CnosDBColumnValue c);

    void visit(CnosDBPrefixOperation op);

    void visit(CnosDBSelect op);

    void visit(CnosDBOrderByTerm op);

    void visit(CnosDBFunction f);

    void visit(CnosDBCastOperation cast);

    void visit(CnosDBBetweenOperation op);

    void visit(CnosDBInOperation op);

    void visit(CnosDBPostfixText op);

    void visit(CnosDBAggregate op);

    void visit(CnosDBFromTable from);

    void visit(CnosDBSubquery subquery);

    void visit(CnosDBBinaryLogicalOperation op);

    void visit(CnosDBLikeOperation op);

    void visit(CnosDBSimilarTo op);

    default void visit(CnosDBExpression expression) {
        if (expression instanceof CnosDBConstant) {
            visit((CnosDBConstant) expression);
        } else if (expression instanceof CnosDBPostfixOperation) {
            visit((CnosDBPostfixOperation) expression);
        } else if (expression instanceof CnosDBColumnValue) {
            visit((CnosDBColumnValue) expression);
        } else if (expression instanceof CnosDBPrefixOperation) {
            visit((CnosDBPrefixOperation) expression);
        } else if (expression instanceof CnosDBSelect) {
            visit((CnosDBSelect) expression);
        } else if (expression instanceof CnosDBOrderByTerm) {
            visit((CnosDBOrderByTerm) expression);
        } else if (expression instanceof CnosDBFunction) {
            visit((CnosDBFunction) expression);
        } else if (expression instanceof CnosDBCastOperation) {
            visit((CnosDBCastOperation) expression);
        } else if (expression instanceof CnosDBBetweenOperation) {
            visit((CnosDBBetweenOperation) expression);
        } else if (expression instanceof CnosDBInOperation) {
            visit((CnosDBInOperation) expression);
        } else if (expression instanceof CnosDBAggregate) {
            visit((CnosDBAggregate) expression);
        } else if (expression instanceof CnosDBPostfixText) {
            visit((CnosDBPostfixText) expression);
        } else if (expression instanceof CnosDBSimilarTo) {
            visit((CnosDBSimilarTo) expression);
        } else if (expression instanceof CnosDBFromTable) {
            visit((CnosDBFromTable) expression);
        } else if (expression instanceof CnosDBSubquery) {
            visit((CnosDBSubquery) expression);
        } else if (expression instanceof CnosDBLikeOperation) {
            visit((CnosDBLikeOperation) expression);
        } else {
            throw new AssertionError(expression);
        }
    }

}
