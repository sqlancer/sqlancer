package sqlancer.mysql;

import sqlancer.mysql.MySQLSchema.MySQLCompositeDataType;
import sqlancer.mysql.ast.MySQLAggregate;
import sqlancer.mysql.ast.MySQLAlias;
import sqlancer.mysql.ast.MySQLAllOperator;
import sqlancer.mysql.ast.MySQLAnyOperator;
import sqlancer.mysql.ast.MySQLBetweenOperation;
import sqlancer.mysql.ast.MySQLBinaryComparisonOperation;
import sqlancer.mysql.ast.MySQLBinaryLogicalOperation;
import sqlancer.mysql.ast.MySQLBinaryOperation;
import sqlancer.mysql.ast.MySQLCastOperation;
import sqlancer.mysql.ast.MySQLCollate;
import sqlancer.mysql.ast.MySQLColumnReference;
import sqlancer.mysql.ast.MySQLComputableFunction;
import sqlancer.mysql.ast.MySQLConstant;
import sqlancer.mysql.ast.MySQLExists;
import sqlancer.mysql.ast.MySQLExpression;
import sqlancer.mysql.ast.MySQLExpressionBag;
import sqlancer.mysql.ast.MySQLInOperation;
import sqlancer.mysql.ast.MySQLJoin;
import sqlancer.mysql.ast.MySQLOrderByTerm;
import sqlancer.mysql.ast.MySQLResultMap;
import sqlancer.mysql.ast.MySQLSelect;
import sqlancer.mysql.ast.MySQLStringExpression;
import sqlancer.mysql.ast.MySQLTableAndColumnReference;
import sqlancer.mysql.ast.MySQLTableReference;
import sqlancer.mysql.ast.MySQLText;
import sqlancer.mysql.ast.MySQLUnaryPostfixOperation;
import sqlancer.mysql.ast.MySQLValues;
import sqlancer.mysql.ast.MySQLValuesRow;
import sqlancer.mysql.ast.MySQLWithClause;

public interface MySQLVisitor {

    void visit(MySQLTableReference ref);

    void visit(MySQLConstant constant);

    void visit(MySQLColumnReference column);

    void visit(MySQLUnaryPostfixOperation column);

    void visit(MySQLComputableFunction f);

    void visit(MySQLBinaryLogicalOperation op);

    void visit(MySQLSelect select);

    void visit(MySQLBinaryComparisonOperation op);

    void visit(MySQLCastOperation op);

    void visit(MySQLInOperation op);

    void visit(MySQLBinaryOperation op);

    void visit(MySQLOrderByTerm op);

    void visit(MySQLExists op);

    void visit(MySQLStringExpression op);

    void visit(MySQLBetweenOperation op);

    void visit(MySQLCollate collate);

    void visit(MySQLJoin join);

    void visit(MySQLText text);

    void visit(MySQLCompositeDataType type);

    // CODDTest
    void visit(MySQLExpressionBag bag);
    void visit(MySQLValues values);
    void visit(MySQLTableAndColumnReference tcreference);
    void visit(MySQLWithClause with);
    void visit(MySQLValuesRow vtable);
    void visit(MySQLAlias alias);
    void visit(MySQLAggregate aggr);
    void visit(MySQLResultMap tSummary);
    void visit(MySQLAllOperator expr);
    void visit(MySQLAnyOperator expr);


    default void visit(MySQLExpression expr) {
        if (expr instanceof MySQLConstant) {
            visit((MySQLConstant) expr);
        } else if (expr instanceof MySQLColumnReference) {
            visit((MySQLColumnReference) expr);
        } else if (expr instanceof MySQLUnaryPostfixOperation) {
            visit((MySQLUnaryPostfixOperation) expr);
        } else if (expr instanceof MySQLComputableFunction) {
            visit((MySQLComputableFunction) expr);
        } else if (expr instanceof MySQLBinaryLogicalOperation) {
            visit((MySQLBinaryLogicalOperation) expr);
        } else if (expr instanceof MySQLSelect) {
            visit((MySQLSelect) expr);
        } else if (expr instanceof MySQLBinaryComparisonOperation) {
            visit((MySQLBinaryComparisonOperation) expr);
        } else if (expr instanceof MySQLCastOperation) {
            visit((MySQLCastOperation) expr);
        } else if (expr instanceof MySQLInOperation) {
            visit((MySQLInOperation) expr);
        } else if (expr instanceof MySQLBinaryOperation) {
            visit((MySQLBinaryOperation) expr);
        } else if (expr instanceof MySQLOrderByTerm) {
            visit((MySQLOrderByTerm) expr);
        } else if (expr instanceof MySQLExists) {
            visit((MySQLExists) expr);
        } else if (expr instanceof MySQLJoin) {
            visit((MySQLJoin) expr);
        } else if (expr instanceof MySQLStringExpression) {
            visit((MySQLStringExpression) expr);
        } else if (expr instanceof MySQLBetweenOperation) {
            visit((MySQLBetweenOperation) expr);
        } else if (expr instanceof MySQLTableReference) {
            visit((MySQLTableReference) expr);
        } else if (expr instanceof MySQLCollate) {
            visit((MySQLCollate) expr);
        } else if (expr instanceof MySQLText) {
            visit((MySQLText) expr);
        } else if (expr instanceof MySQLExpressionBag) {
            visit((MySQLExpressionBag) expr);
        } else if (expr instanceof MySQLValues) {
            visit((MySQLValues) expr);
        } else if (expr instanceof MySQLTableAndColumnReference) {
            visit((MySQLTableAndColumnReference) expr);
        } else if (expr instanceof MySQLWithClause) {
            visit((MySQLWithClause) expr);
        } else if (expr instanceof MySQLValuesRow) {
            visit((MySQLValuesRow) expr);
        } else if (expr instanceof MySQLAlias) {
            visit((MySQLAlias) expr);
        } else if (expr instanceof MySQLAggregate) {
            visit((MySQLAggregate) expr);
        } else if (expr instanceof MySQLResultMap) {
            visit((MySQLResultMap) expr);
        } else if (expr instanceof MySQLAllOperator) {
            visit((MySQLAllOperator) expr);
        } else if (expr instanceof MySQLAnyOperator) {
            visit((MySQLAnyOperator) expr);
        } else if (expr instanceof MySQLCompositeDataType) {
            visit((MySQLCompositeDataType) expr);
        } else {
            throw new AssertionError(expr);
        }
    }

    static String asString(MySQLExpression expr) {
        MySQLToStringVisitor visitor = new MySQLToStringVisitor();
        visitor.visit(expr);
        return visitor.get();
    }

    static String asExpectedValues(MySQLExpression expr) {
        MySQLExpectedValueVisitor visitor = new MySQLExpectedValueVisitor();
        visitor.visit(expr);
        return visitor.get();
    }

}
